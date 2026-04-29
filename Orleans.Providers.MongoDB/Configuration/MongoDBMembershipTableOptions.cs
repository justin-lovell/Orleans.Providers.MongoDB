// ReSharper disable InheritdocConsiderUsage

namespace Orleans.Providers.MongoDB.Configuration
{
    /// <summary>
    /// Configures MongoDB Membership.
    /// </summary>
    public sealed class MongoDBMembershipTableOptions : MongoDBOptions
    {
        public MongoDBMembershipStrategy Strategy { get; set; }

        /// <summary>
        /// Determines whether a TTL index is created and used in the MongoDB collection for cluster membership entries.
        /// When enabled, entries in the collection are subject to automatic expiration based on a system-defined time-to-live (TTL)
        /// as configured in <see cref="Orleans.Configuration.ClusterMembershipOptions.DefunctSiloExpiration"/>.
        /// </summary>
        /// <remarks>
        /// Enabling this property can help automatically clean up expired cluster membership data in MongoDB.
        /// This behavior depends on the TTL feature of MongoDB and the proper initialization of the TTL index in the collection.
        /// </remarks>
        public bool UseMongoTtlIndex { get; set; }

        public MongoDBMembershipTableOptions()
        {
        }
    }
}
