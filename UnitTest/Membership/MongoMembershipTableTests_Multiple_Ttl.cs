using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Providers.MongoDB.Configuration;
using Orleans.Providers.MongoDB.Membership;
using Orleans.Providers.MongoDB.UnitTest.Fixtures;
using Orleans.Runtime;
using TestExtensions;
using UnitTests;
using UnitTests.MembershipTests;
using Xunit;

namespace Orleans.Providers.MongoDB.UnitTest.Membership;

[TestCategory("Membership")]
[TestCategory("Mongo")]
public class MongoMembershipTableTests_Multiple_Ttl : MembershipTableTestsBase
{
    private static readonly TimeSpan MonitorTtlInterval = TimeSpan.FromSeconds(1);
    private static int _borrowedGeneration;
    
    public MongoMembershipTableTests_Multiple_Ttl(ConnectionStringFixture fixture, TestEnvironmentFixture environment)
        : base(fixture, environment, new LoggerFilterOptions())
    {
    }

    private static void SetIndexMonitorFrequency()
    {
        var client = MongoDatabaseFixture.ReplicaSetFactory.Create(nameof(MongoMembershipTableTests_Multiple_Ttl));
        var adminDb = client.GetDatabase("admin");

        adminDb.RunCommand(new BsonDocumentCommand<BsonDocument>(new BsonDocument
        {
            { "setParameter", 1 },
            { "ttlMonitorSleepSecs", MonitorTtlInterval.TotalSeconds }
        }));
    }

    protected override IMembershipTable CreateMembershipTable(ILogger logger)
    {
        SetIndexMonitorFrequency();
        
        var options = Options.Create(new MongoDBMembershipTableOptions
        {
            CollectionPrefix = "TestTtl_",
            DatabaseName = "OrleansTest",
            Strategy = MongoDBMembershipStrategy.Multiple,
            UseMongoTtlIndex = true
        });

        return new MongoMembershipTable(
            MongoDatabaseFixture.ReplicaSetFactory,
            loggerFactory.CreateLogger<MongoMembershipTable>(),
            _clusterOptions,
            Options.Create(new ClusterMembershipOptions()),
            options);
    }

    protected override IGatewayListProvider CreateGatewayListProvider(ILogger logger)
    {
        SetIndexMonitorFrequency();
        
        var options = Options.Create(new MongoDBGatewayListProviderOptions
        {
            CollectionPrefix = "TestTtl_",
            DatabaseName = "OrleansTest",
            Strategy = MongoDBMembershipStrategy.Multiple,
            UseMongoTtlIndex = true
        });

        return new MongoGatewayListProvider(
            MongoDatabaseFixture.ReplicaSetFactory,
            loggerFactory.CreateLogger<MongoGatewayListProvider>(),
            _clusterOptions,
            Options.Create(new ClusterMembershipOptions()),
            _gatewayOptions,
            options);
    }

    protected override Task<string> GetConnectionString()
    {
        return Task.FromResult(MongoDatabaseFixture.ReplicaSetConnectionString);
    }

    [Fact(Skip = "Assertions made in scenario has false positives due to Mongo TTL dropping"), TestCategory("Functional")]
    public async Task Test_CleanupDefunctSiloEntries()
    {
        await MembershipTable_CleanupDefunctSiloEntries();
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_GetGateways()
    {
        await MembershipTable_GetGateways();
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_ReadAll_EmptyTable()
    {
        await MembershipTable_ReadAll_EmptyTable();
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_InsertRow()
    {
        await MembershipTable_InsertRow(true);
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_ReadRow_Insert_Read()
    {
        await MembershipTable_ReadRow_Insert_Read(true);
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_ReadAll_Insert_ReadAll()
    {
        await MembershipTable_ReadAll_Insert_ReadAll(true);
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_UpdateRow()
    {
        await MembershipTable_UpdateRow(true);
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_UpdateRowInParallel()
    {
        await MembershipTable_UpdateRowInParallel(true);
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_UpdateIAmAlive()
    {
        await MembershipTable_UpdateIAmAlive(true);
    }

    [Fact, TestCategory("Functional")]
    public async Task Test_VerifyMongoIndexTtl_IsEffective()
    {
        // inspired by MembershipTable_CleanupDefunctSiloEntries scenario
        // note that the CleanupDefunctSiloEntries is not called explicitly. Collection is cleaned up by TTL index
        
        var membershipTable = CreateMembershipTable(NullLogger<MongoMembershipTable>.Instance);
        await membershipTable.InitializeMembershipTable(true).WaitAsync(TimeSpan.FromMinutes(1));
        
        MembershipTableData data = await membershipTable.ReadAll();
        TableVersion newTableVersion = data.Version.Next();
        
        var oldEntryDead = CreateMembershipEntryForTest();
        oldEntryDead.IAmAliveTime = oldEntryDead.IAmAliveTime.AddDays(-10);
        oldEntryDead.StartTime = oldEntryDead.StartTime.AddDays(-10);
        oldEntryDead.Status = SiloStatus.Dead;
        bool ok = await membershipTable.InsertRow(oldEntryDead, newTableVersion);
        var table = await membershipTable.ReadAll();

        Assert.True(ok, "InsertRow Dead failed");

        newTableVersion = table.Version.Next();
        var oldEntryJoining = CreateMembershipEntryForTest();
        oldEntryJoining.IAmAliveTime = oldEntryJoining.IAmAliveTime.AddDays(-10);
        oldEntryJoining.StartTime = oldEntryJoining.StartTime.AddDays(-10);
        oldEntryJoining.Status = SiloStatus.Joining;
        ok = await membershipTable.InsertRow(oldEntryJoining, newTableVersion);
        table = await membershipTable.ReadAll();

        Assert.True(ok, "InsertRow Joining failed");
        
        newTableVersion = table.Version.Next();
        var staleActiveEntry = CreateMembershipEntryForTest();
        staleActiveEntry.IAmAliveTime = staleActiveEntry.IAmAliveTime.AddDays(-10);
        staleActiveEntry.StartTime = staleActiveEntry.StartTime.AddDays(-10);
        staleActiveEntry.Status = SiloStatus.Active;
        ok = await membershipTable.InsertRow(staleActiveEntry, newTableVersion);
        table = await membershipTable.ReadAll();

        Assert.True(ok, "InsertRow Stale Active failed");
        
        newTableVersion = table.Version.Next();
        var  newEntry = CreateMembershipEntryForTest();
        ok = await membershipTable.InsertRow(newEntry, newTableVersion);

        Assert.True(ok, "InsertRow failed");
        
        // we have to wait for the mongo background monitor to trigger. add some buffer
        await Task.Delay(MonitorTtlInterval + TimeSpan.FromSeconds(5));
        
        data = await membershipTable.ReadAll();
        Assert.Single(data.Members);
        
        return;

        static MembershipEntry CreateMembershipEntryForTest()
        {
            SiloAddress siloAddress = CreateSiloAddressForTest();

            var membershipEntry = new MembershipEntry
            {
                SiloAddress = siloAddress,
                HostName = Dns.GetHostName(),
                SiloName = "TestSiloName",
                Status = SiloStatus.Joining,
                ProxyPort = siloAddress.Endpoint.Port,
                StartTime = GetUtcNowWithSecondsResolution(),
                IAmAliveTime = GetUtcNowWithSecondsResolution()
            };

            return membershipEntry;
        }
        
        static DateTime GetUtcNowWithSecondsResolution()
        {
            var now = DateTime.UtcNow;
            return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, DateTimeKind.Utc);
        }
        
        static SiloAddress CreateSiloAddressForTest()
        {
            var siloAddress = SiloAddressUtils.NewLocalSiloAddress(Interlocked.Increment(ref _borrowedGeneration));
            siloAddress.Endpoint.Port = 54321;
            return siloAddress;
        }
    }
}