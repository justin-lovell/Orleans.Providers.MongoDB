// ReSharper disable InheritdocConsiderUsage

namespace Orleans.Providers.MongoDB.Configuration
{
    /// <summary>
    /// Configures MongoDB Gateway List Provider.
    /// </summary>
    public sealed class MongoDBGatewayListProviderOptions : MongoDBOptions
    {
        public MongoDBMembershipStrategy Strategy { get; set; }

        /// <summary>
        /// Specifies whether a TTL (Time-to-Live) index should be utilized in MongoDB for managing gateway entries.
        /// When enabled, the TTL index automatically removes expired gateway records from the collection
        /// based on the configured expiration settings in
        /// <see cref="Orleans.Configuration.ClusterMembershipOptions.DefunctSiloExpiration"/>
        /// </summary>
        public bool UseMongoTtlIndex { get; set; }

        public MongoDBGatewayListProviderOptions()
        {
        }
    }
}
