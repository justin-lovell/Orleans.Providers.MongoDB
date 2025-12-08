using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Providers.MongoDB.Reminders.Store
{
    public interface IMongoReminderCollection
    {
        Task<ReminderTableData> ReadRowsInRange(uint beginHash, uint endHash);
        Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName);
        Task<ReminderTableData> ReadRow(GrainId grainId);
        Task<ReminderTableData> ReadRowsOutRange(uint beginHash, uint endHash);
        Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag);
        Task RemoveRows();
        Task<string> UpsertRow(ReminderEntry entry);
    }
}