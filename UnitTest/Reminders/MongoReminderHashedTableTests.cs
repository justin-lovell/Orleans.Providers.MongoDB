using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Providers.MongoDB.Configuration;
using Orleans.Providers.MongoDB.Reminders;
using Orleans.Providers.MongoDB.UnitTest.Fixtures;
using TestExtensions;
using UnitTests;
using UnitTests.RemindersTest;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Providers.MongoDB.UnitTest.Reminders
{
    [TestCategory("Reminders")]
    [TestCategory("Mongo")]
    public class MongoReminderHashedTableTests : ReminderTableTestsBase
    {
        private readonly ITestOutputHelper testOutputHelper;
        private MongoClientJig mongoClientFixture;

        public MongoReminderHashedTableTests(ConnectionStringFixture fixture, TestEnvironmentFixture clusterFixture, ITestOutputHelper testOutputHelper)
            : base(fixture, clusterFixture, new LoggerFilterOptions())
        {
            this.testOutputHelper = testOutputHelper;
        }

        protected override IReminderTable CreateRemindersTable()
        {
            mongoClientFixture ??= new MongoClientJig();
            
            var options = Options.Create(new MongoDBRemindersOptions
            {
                CollectionPrefix = $"TestHashed__{Random.Shared.Next()}",
                DatabaseName = "OrleansTest",
                Strategy = MongoDBReminderStrategy.HashedLookupStorage
            });

            return new MongoReminderTable(
                mongoClientFixture.CreateDatabaseFactory(),
                loggerFactory.CreateLogger<MongoReminderTable>(),
                options,
                clusterOptions);
        }

        protected override Task<string> GetConnectionString()
        {
            return Task.FromResult(MongoDatabaseFixture.DatabaseConnectionString);
        }

        [Fact]
        public async Task Test_RemindersRange()
        {
            await RemindersRange(50);
            await mongoClientFixture.AssertQualityChecksAsync(testOutputHelper);
        }

        [Fact]
        public async Task Test_RemindersParallelUpsert()
        {
            await RemindersParallelUpsert();
            await mongoClientFixture.AssertQualityChecksAsync(testOutputHelper);
        }

        [Fact]
        public async Task Test_ReminderSimple()
        {
            await ReminderSimple();
            await mongoClientFixture.AssertQualityChecksAsync(testOutputHelper);
        }

        [SkippableFact]
        public async Task Test_RemindersTotalDuration()
        {
            Skip.IfNot(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CI")));
            
            // warm-up, create indexes, ect.
            await ReminderSimple();
            await ReminderSimple();
            
            var stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < 5000; i++)
            {
                await ReminderSimple();
            }
            
            await RemindersRange(50);
                
            stopwatch.Stop();
            testOutputHelper.WriteLine($"Total elapsed time: {stopwatch.Elapsed}");
            await mongoClientFixture.AssertQualityChecksAsync(testOutputHelper);
        }
    }
}
