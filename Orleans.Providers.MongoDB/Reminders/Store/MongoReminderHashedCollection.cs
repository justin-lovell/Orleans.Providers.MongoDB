using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using Orleans.Providers.MongoDB.Utils;
using Orleans.Runtime;

// ReSharper disable RedundantIfElseBlock

namespace Orleans.Providers.MongoDB.Reminders.Store
{
    /// <summary>
    ///     <p>
    ///         Represents a MongoDB-based implementation of a reminder collection utilizing hashed key retrieval strategy.
    ///         This class is responsible for managing reminder data by reading, writing, and removing rows, as well
    ///         as setting up the collection schema and indexes for efficient querying.
    ///     </p>
    ///     <p>
    ///         Removal of the reminder is a single delete operation, unlike the <see cref="MongoReminderCollection"/> which
    ///         will perform a two-stage removal.
    ///     </p>
    ///     <p>
    ///         A low cardinality of reminders per grain has been assumed.
    ///     </p>
    /// </summary>
    public class MongoReminderHashedCollection : CollectionBase<MongoReminderDocument>, IMongoReminderCollection
    {
        private readonly string serviceId;
        private readonly string collectionPrefix;

        public MongoReminderHashedCollection(
            IMongoClient mongoClient,
            string databaseName,
            string collectionPrefix,
            Action<MongoCollectionSettings> collectionConfigurator,
            bool createShardKey,
            string serviceId)
            : base(mongoClient, databaseName, collectionConfigurator, createShardKey)
        {
            this.serviceId = serviceId;
            this.collectionPrefix = collectionPrefix;
        }

        protected override string CollectionName()
        {
            return collectionPrefix + "OrleansReminderV2";
        }

        protected override void SetupCollection(IMongoCollection<MongoReminderDocument> collection)
        {
            var byHashDefinition =
                Index
                    .Ascending(x => x.ServiceId)
                    .Ascending(x => x.GrainHash);
            try
            {
                collection.Indexes.CreateOne(
                    new CreateIndexModel<MongoReminderDocument>(byHashDefinition,
                        new CreateIndexOptions
                        {
                            Name = "ByGrainHash"
                        }));
            }
            catch (MongoCommandException ex)
            {
                if (ex.CodeName == "IndexOptionsConflict")
                {
                    collection.Indexes.CreateOne(new CreateIndexModel<MongoReminderDocument>(byHashDefinition));
                }
            }
        }

        public virtual async Task<ReminderTableData> ReadRowsInRange(uint beginHash, uint endHash)
        {
            var reminders =
                await Collection.Find(x =>
                        x.ServiceId == serviceId &&
                        x.GrainHash > beginHash &&
                        x.GrainHash <= endHash)
                    .ToListAsync();

            return new ReminderTableData(reminders.Select(x => x.ToEntry()));
        }

        public virtual async Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
        {
            var id = ReturnId(serviceId, grainId, reminderName);
            var reminder =
                await Collection.Find(x => x.Id == id)
                    .FirstOrDefaultAsync();

            return reminder?.ToEntry();
        }

        public virtual async Task<ReminderTableData> ReadRowsOutRange(uint beginHash, uint endHash)
        {
            var reminders =
                await Collection.Find(x =>
                        x.ServiceId == serviceId &&
                        (x.GrainHash <= beginHash || x.GrainHash > endHash))
                    .ToListAsync();

            return new ReminderTableData(reminders.Select(x => x.ToEntry()));
        }

        public virtual async Task<ReminderTableData> ReadRow(GrainId grainId)
        {
            var reminders =
                await Collection.Find(r =>
                        r.ServiceId == serviceId &&
                        r.GrainHash == grainId.GetUniformHashCode() &&
                        r.GrainId == grainId.ToString())
                    .ToListAsync();

            return new ReminderTableData(reminders.Select(x => x.ToEntry()));
        }

        public async Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        {
            var id = ReturnId(serviceId, grainId, reminderName);

            try
            {
                var deleteResult = await Collection.DeleteOneAsync(x => x.Id == id && x.Etag == eTag);
                return deleteResult.DeletedCount > 0;
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

        public virtual Task RemoveRows()
        {
            // note: only used and called by the test harness
            return Collection.DeleteManyAsync(r => true);
        }

        public virtual async Task<string> UpsertRow(ReminderEntry entry)
        {
            var id = ReturnId(serviceId, entry.GrainId, entry.ReminderName);
            var updatedEtag = Guid.NewGuid().ToString();

            try
            {
                var result = await Collection.ReplaceOneAsync(
                    // ReSharper disable once EqualExpressionComparison -- etag will only mutate after update
                    x => x.Id == id && entry.ETag == entry.ETag,
                    MongoReminderDocument.Create(id, serviceId, entry, updatedEtag), 
                    UpsertReplace
                );

                if (result.MatchedCount == 0)
                {
                    // we won't change the etag and notify nothing changed by returning null
                    return null;
                }
            }
            catch (MongoException ex)
            {
                if (!ex.IsDuplicateKey())
                {
                    throw;
                }
            }

            entry.ETag = updatedEtag;
            return entry.ETag;
        }

        private static string ReturnId(string serviceId, GrainId grainId, string reminderName)
        {
            return $"{serviceId}_{grainId}_{reminderName}";
        }
    }
}