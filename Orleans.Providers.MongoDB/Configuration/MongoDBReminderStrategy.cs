namespace Orleans.Providers.MongoDB.Configuration;

/// <summary>
/// Specifies the storage strategy for MongoDB-based reminders in Orleans.
/// </summary>
public enum MongoDBReminderStrategy
{
    /// <summary>
    /// The standard classic strategy.
    /// </summary>
    StandardStorage,
    
    /// <summary>
    /// <p>Implements a storage strategy indexed by grain hash with efficient, single-shot upsert and delete commands.</p>
    /// <p>Use this strategy when the system requires scaling to a high volume of grains, provided the reminder count per grain remains low.</p>
    /// </summary>
    HashedLookupStorage
}