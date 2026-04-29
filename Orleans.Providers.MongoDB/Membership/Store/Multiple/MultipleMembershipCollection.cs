using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Orleans.Providers.MongoDB.Utils;
using Orleans.Runtime;

namespace Orleans.Providers.MongoDB.Membership.Store.Multiple
{
    public sealed class MultipleMembershipCollection : CollectionBase<MongoMembershipDocument>, IMongoMembershipCollection
    {
        private readonly TableVersionCollection tableVersionCollection;
        private readonly string collectionPrefix;

        public MultipleMembershipCollection(
            IMongoClient mongoClient,
            string databaseName,
            string collectionPrefix,
            Action<MongoCollectionSettings> collectionConfigurator,
            bool createShardKey)
            : base(mongoClient, databaseName, collectionConfigurator, createShardKey)
        {
            this.collectionPrefix = collectionPrefix;

            tableVersionCollection = new TableVersionCollection(mongoClient, databaseName, collectionPrefix, collectionConfigurator, createShardKey);
        }

        protected override string CollectionName()
        {
            return $"{collectionPrefix}OrleansMembershipV3";
        }

        protected override void SetupCollection(IMongoCollection<MongoMembershipDocument> collection)
        {
            var byDeploymentIdDefinition = Index.Ascending(x => x.DeploymentId);

            collection.Indexes.CreateOne(
                new CreateIndexModel<MongoMembershipDocument>(byDeploymentIdDefinition,
                    new CreateIndexOptions
                    {
                        Name = "ByDeploymentId"
                    }));
        }

        public async Task<bool> UpsertRow(string deploymentId, MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            using var session = await Client.StartSessionAsync();
            return await session.WithTransactionAsync(async (sessionHandle, ct) =>
            {
                var hasUpsertedTable = await tableVersionCollection.UpsertAsync(sessionHandle, tableVersion, deploymentId);

                if (!hasUpsertedTable)
                {
                    await sessionHandle.AbortTransactionAsync(ct);
                    return false;
                }

                var hasUpsertedMember = await UpsertRowAsync(sessionHandle, deploymentId, entry, etag);

                var isOkay = hasUpsertedMember && hasUpsertedTable;

                if (!isOkay)
                {
                    await sessionHandle.AbortTransactionAsync(ct);
                }

                return isOkay;
            });
        }

        public Task InitializeTtl(string clusterId, TimeSpan? timeToLive)
        {
            var indexName = $"ttl_{Convert.ToHexString(SHA1.HashData(Encoding.UTF8.GetBytes(clusterId)))}";

            return timeToLive == null
                ? DropExistingIndexIfNeeded()
                : UpdateTtlIndexPolicy(timeToLive.Value);

            async Task DropExistingIndexIfNeeded()
            {
                try
                {
                    // drop the index if it was already created in the past, and the cluster settings revert to explicit
                    await Collection.Indexes.DropOneAsync(indexName);
                }
                catch (MongoCommandException e) when (e.IsIndexMissing())
                {
                    // do nothing since the index is already dropped
                }
            }

            async Task UpdateTtlIndexPolicy(TimeSpan ttl)
            {
                var safeTimeToLive = Math.Round(ttl.TotalSeconds, MidpointRounding.AwayFromZero);

                try
                {
                    // in the event that the index already exists, we need to update the TTL policy due to a cluster
                    // configuration modification
                    var updateExpireAfterCommand = new BsonDocument()
                    {
                        { "collMod", CollectionName() },
                        {
                            "index", new BsonDocument
                            {
                                { "name", indexName },
                                { "expireAfterSeconds", safeTimeToLive },
                            }
                        },
                    };
                    await Database.RunCommandAsync(new BsonDocumentCommand<BsonDocument>(updateExpireAfterCommand));
                }
                catch (MongoCommandException e) when (e.IsIndexMissing())
                {
                    var keysDefinition = Index.Ascending(x => x.Timestamp);
                    // ensure that index ttl only applies to documents within the cluster deployment id
                    var partialFilterExpression = Filter.Eq(x => x.DeploymentId, clusterId);
                    var indexOptions = new CreateIndexOptions<MongoMembershipDocument>
                    {
                        Name = indexName,
                        ExpireAfter = TimeSpan.FromSeconds(safeTimeToLive),
                        PartialFilterExpression = partialFilterExpression,
                    };

                    await Collection.Indexes.CreateOneAsync(
                        new CreateIndexModel<MongoMembershipDocument>(keysDefinition, indexOptions));
                }
            }
        }

        private async Task<bool> UpsertRowAsync(IClientSessionHandle session, string deploymentId, MembershipEntry entry, string etag)
        {
            var id = ReturnId(deploymentId, entry.SiloAddress);

            var document = MongoMembershipDocument.Create(entry, deploymentId, id);

            try
            {
                await Collection.ReplaceOneAsync(session, x => x.Id == id && x.Etag == etag, document, UpsertReplace);

                return true;
            }
            catch (MongoException ex)
            {
                if (ex.IsDuplicateKey())
                {
                    return false;
                }
                throw;
            }
        }

        public async Task<IList<Uri>> GetGateways(string deploymentId)
        {
            var entries =
                await Collection.Find(x => x.DeploymentId == deploymentId && x.Status == (int)SiloStatus.Active && x.ProxyPort > 0)
                    .ToListAsync();

            return entries.Select(x => x.ToGatewayUri()).ToList();
        }

        public async Task<MembershipTableData> ReadAll(string deploymentId)
        {
            using var session = await Client.StartSessionAsync();
            
            var tableVersion = await tableVersionCollection.GetTableVersionAsync(session, deploymentId);

            var entries =
                await Collection.Find(session, x => x.DeploymentId == deploymentId).ToListAsync();

            return ReturnMembershipTableData(entries, tableVersion);
        }

        public async Task<MembershipTableData> ReadRow(string deploymentId, SiloAddress address)
        {
            using var session = await Client.StartSessionAsync();
            
            var tableVersion = await tableVersionCollection.GetTableVersionAsync(session, deploymentId);

            var id = ReturnId(deploymentId, address);

            var entries =
                await Collection.Find(session, x => x.Id == id).ToListAsync();

            return ReturnMembershipTableData(entries, tableVersion);
            
        }

        public Task UpdateIAmAlive(string deploymentId, SiloAddress address, DateTime iAmAliveTime)
        {
            var id = ReturnId(deploymentId, address);

            return Collection.UpdateOneAsync(x => x.Id == id, Update.Set(x => x.IAmAliveTime, LogFormatter.PrintDate(iAmAliveTime)));
        }

        public Task CleanupDefunctSiloEntries(string deploymentId, DateTimeOffset beforeDate)
        {
            var beforeUtc = beforeDate.UtcDateTime;

            return Collection.DeleteManyAsync(x => x.DeploymentId == deploymentId && x.Status != (int)SiloStatus.Active && x.Timestamp < beforeUtc);
        }

        public async Task DeleteMembershipTableEntries(string deploymentId)
        {
            using var session = await Client.StartSessionAsync();

            await session.WithTransactionAsync(async (sessionHandle, ct) =>
            {
                await Collection.DeleteManyAsync(sessionHandle, x => x.DeploymentId == deploymentId);
                await tableVersionCollection.DeleteAsync(sessionHandle, deploymentId);
                return true;
            });
        }

        private static MembershipTableData ReturnMembershipTableData(IEnumerable<MongoMembershipDocument> membershipList, TableVersion tableVersion)
        {
            return new MembershipTableData(membershipList.Select(x => Tuple.Create(x.ToEntry(), x.Etag)).ToList(), tableVersion);
        }

        private static string ReturnAddress(IPAddress address)
        {
            return address.MapToIPv4().ToString();
        }

        private static string ReturnId(string deploymentId, SiloAddress address)
        {
            return $"{deploymentId}@{ReturnAddress(address.Endpoint.Address)}:{address.Endpoint.Port}/{address.Generation}";
        }
    }
}
