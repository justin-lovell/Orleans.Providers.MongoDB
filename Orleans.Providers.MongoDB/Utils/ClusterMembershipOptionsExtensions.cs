using System;
using Orleans.Configuration;

namespace Orleans.Providers.MongoDB.Utils;

internal static class ClusterMembershipOptionsExtensions
{
    /// <summary>
    /// Retrieves the TTL (Time-to-Live) timespan for use in MongoDB TTL indexes based on the provided
    /// cluster membership options and a flag indicating whether TTL indexing should be used.
    /// </summary>
    /// <param name="options">The cluster membership options containing TTL-related configuration.</param>
    /// <param name="useMongoTtlIndex">Indicates whether a MongoDB TTL index should be utilized.</param>
    /// <returns>
    /// A <see cref="TimeSpan"/> representing the TTL duration if <paramref name="useMongoTtlIndex"/> is true;
    /// otherwise, null.
    /// </returns>
    public static TimeSpan? GetMongoTtlTimeSpan(this ClusterMembershipOptions options, bool useMongoTtlIndex)
    {
        ArgumentNullException.ThrowIfNull(options);
        return useMongoTtlIndex ? options.DefunctSiloExpiration : null;
    }
}