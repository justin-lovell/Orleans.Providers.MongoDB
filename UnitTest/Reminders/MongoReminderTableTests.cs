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

namespace Orleans.Providers.MongoDB.UnitTest.Reminders
{
    [TestCategory("Reminders")]
    [TestCategory("Mongo")]
    public class MongoReminderTableTests : ReminderTableTestsBase
    {
        public MongoReminderTableTests(ConnectionStringFixture fixture, TestEnvironmentFixture clusterFixture)
            : base(fixture, clusterFixture, new LoggerFilterOptions())
        {
        }

        protected override IReminderTable CreateRemindersTable()
        {
            var options = Options.Create(new MongoDBRemindersOptions
            {
                CollectionPrefix = $"Test_{Random.Shared.Next()}",
                DatabaseName = "OrleansTest",
                Strategy = MongoDBReminderStrategy.StandardStorage
            });

            return new MongoReminderTable(
                MongoDatabaseFixture.DatabaseFactory,
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
        }

        [Fact]
        public async Task Test_RemindersParallelUpsert()
        {
            await RemindersParallelUpsert();
        }

        [Fact]
        public async Task Test_ReminderSimple()
        {
            await ReminderSimple();
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
