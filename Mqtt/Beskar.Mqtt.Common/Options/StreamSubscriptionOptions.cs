using System.Threading.Channels;

namespace Beskar.Mqtt.Common.Options;

/// <summary>
/// Configuration options for MQTT stream subscriptions (<see cref="IAsyncEnumerable{T}"/>).
/// </summary>
public sealed class StreamSubscriptionOptions
{
   /// <summary>
   /// Default stream subscription options (BoundedCapacity = 1024, FullMode = Wait).
   /// </summary>
   public static readonly StreamSubscriptionOptions Default = new();

   /// <summary>
   /// The maximum number of unread messages buffered in the stream channel before backpressure policy applies.
   /// Default is 1024.
   /// </summary>
   public int BoundedCapacity { get; init; } = 1024;

   /// <summary>
   /// The behavior when the channel buffer is full.
   /// Default is <see cref="BoundedChannelFullMode.Wait"/>.
   /// </summary>
   public BoundedChannelFullMode FullMode { get; init; } = BoundedChannelFullMode.Wait;
}
