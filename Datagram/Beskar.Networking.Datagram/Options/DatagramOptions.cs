using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram.Options;

/// <summary>
/// Configuration options for <see cref="DatagramHost"/>.
/// </summary>
public sealed class DatagramOptions
{
   /// <summary>
   /// Maximum UDP payload size in bytes per physical datagram (excluding IP/UDP headers).
   /// Defaults to <c>1200</c> bytes to avoid IP fragmentation over WAN, IPv6, PPPoE, and VPN paths.
   /// </summary>
   public int MaxDatagramSize { get; set; } = 1200;

   /// <summary>
   /// Socket send buffer size in bytes. Set <c>null</c> to use OS defaults. Defaults to 4 MB.
   /// </summary>
   public int? SocketSendBufferSize { get; set; } = 4 * 1024 * 1024;

   /// <summary>
   /// Socket receive buffer size in bytes. Set <c>null</c> to use OS defaults. Defaults to 4 MB.
   /// </summary>
   public int? SocketReceiveBufferSize { get; set; } = 4 * 1024 * 1024;

   /// <summary>
   /// Maximum concurrent peers accepted by this host. Defaults to <c>128</c>.
   /// </summary>
   public int MaxPeers { get; set; } = 128;

   /// <summary>
   /// Controls whether events are polled synchronously or pushed asynchronously.
   /// Defaults to <see cref="DatagramEventDispatchMode.Poll"/>.
   /// </summary>
   public DatagramEventDispatchMode DispatchMode { get; set; } = DatagramEventDispatchMode.Poll;

   /// <summary>
   /// When <c>true</c>, <see cref="DatagramHost.Tick"/> is driven manually by caller code (ideal for fixed game loops).
   /// When <c>false</c>, an internal background timer drives protocol ticks at <see cref="TickInterval"/>.
   /// </summary>
   public bool ManualTick { get; set; } = false;

   /// <summary>
   /// Background tick cadence when <see cref="ManualTick"/> is <c>false</c>. Defaults to 10 ms.
   /// </summary>
   public TimeSpan TickInterval { get; set; } = TimeSpan.FromMilliseconds(10);

   /// <summary>
   /// Interval between handshake packet retries. Defaults to 250 ms.
   /// </summary>
   public TimeSpan HandshakeRetryInterval { get; set; } = TimeSpan.FromMilliseconds(250);

   /// <summary>
   /// Maximum total duration for completing a connection handshake before timing out. Defaults to 5 seconds.
   /// </summary>
   public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

   /// <summary>
   /// Interval at which lightweight ACK/keepalive packets are sent when no data has been sent recently.
   /// Defaults to 250 ms.
   /// </summary>
   public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromMilliseconds(250);

   /// <summary>
   /// Duration without valid incoming packets from a peer before disconnecting it. Defaults to 10 seconds.
   /// </summary>
   public TimeSpan PeerTimeout { get; set; } = TimeSpan.FromSeconds(10);

   /// <summary>
   /// Maximum duration a reliable message will continue retrying before tearing down the peer. Defaults to 12 seconds.
   /// </summary>
   public TimeSpan MaxReliableRetryDuration { get; set; } = TimeSpan.FromSeconds(12);

   /// <summary>
   /// Interval between automatic <c>TimeSyncPing</c> probes for clock-offset estimation.
   /// Set to <see cref="TimeSpan.Zero"/> to disable automatic clock sync. Defaults to 1 second.
   /// </summary>
   public TimeSpan TimeSyncInterval { get; set; } = TimeSpan.FromSeconds(1);

   /// <summary>
   /// Minimum RTO clamp in milliseconds. Defaults to 50 ms.
   /// </summary>
   public double MinRtoMs { get; set; } = 50.0;

   /// <summary>
   /// Maximum RTO clamp in milliseconds. Defaults to 2000 ms.
   /// </summary>
   public double MaxRtoMs { get; set; } = 2000.0;

   /// <summary>
   /// Per-peer outbound pacing limit in bytes per second. Set <c>null</c> to disable pacing cap.
   /// Defaults to 2 MB/s per peer.
   /// </summary>
   public long? MaxBytesPerSecondPerPeer { get; set; } = 2 * 1024 * 1024;

   /// <summary>
   /// Maximum burst bytes allowed by the per-peer token bucket pacer. Defaults to 32 KB.
   /// </summary>
   public int PacingBurstBytes { get; set; } = 32 * 1024;

   /// <summary>
   /// Maximum number of physical UDP data datagrams flushed per peer during a single <see cref="DatagramHost.Tick"/>.
   /// Defaults to <c>32</c>. Increase for high-bandwidth bulk transfers or lower tick frequencies.
   /// </summary>
   public int MaxDatagramsPerTick { get; set; } = 32;

   /// <summary>
   /// Maximum pending events queued in poll/async dispatch queues before applying backpressure.
   /// </summary>
   public int MaxQueuedEvents { get; set; } = 8192;

   /// <summary>
   /// Maximum size of client-supplied connect metadata in bytes. Defaults to 512 bytes.
   /// </summary>
   public int MaxConnectMetadataBytes { get; set; } = 512;

   /// <summary>
   /// Security, encryption, anti-amplification, and migration settings.
   /// </summary>
   public DatagramSecurityOptions Security { get; set; } = new();

   /// <summary>
   /// Registered logical channels (<c>0..63</c>). If left empty, channel <c>0</c> is auto-configured as
   /// <see cref="DatagramDeliveryMode.ReliableOrdered"/>.
   /// </summary>
   public List<DatagramChannelConfig> Channels { get; } = [];

   /// <summary>
   /// Fluently registers a channel configuration.
   /// </summary>
   public DatagramOptions AddChannel(
      byte channelId,
      DatagramDeliveryMode deliveryMode,
      byte priority = 128,
      int windowSize = 512,
      int maxSendQueueDepth = 1024,
      bool? allowFragmentation = null,
      int maxFragmentCount = 128)
   {
      var isReliable = deliveryMode is DatagramDeliveryMode.ReliableUnordered
         or DatagramDeliveryMode.ReliableOrdered
         or DatagramDeliveryMode.ReliableSequenced;

      Channels.Add(new DatagramChannelConfig
      {
         ChannelId = channelId,
         DeliveryMode = deliveryMode,
         Priority = priority,
         WindowSize = windowSize,
         MaxSendQueueDepth = maxSendQueueDepth,
         AllowFragmentation = allowFragmentation ?? isReliable,
         MaxFragmentCount = maxFragmentCount
      });

      return this;
   }

   internal DatagramChannelConfig?[] BuildValidatedChannelTable(out DatagramChannelConfig[] orderedByPriority, out ulong configHash)
   {
      if (MaxDatagramSize is < 256 or > 65000)
      {
         throw new ArgumentOutOfRangeException(nameof(MaxDatagramSize), "MaxDatagramSize must be between 256 and 65000 bytes.");
      }

      if (MaxPeers < 1)
      {
         throw new ArgumentOutOfRangeException(nameof(MaxPeers), "MaxPeers must be at least 1.");
      }

      if (MaxQueuedEvents < 1)
      {
         throw new ArgumentOutOfRangeException(nameof(MaxQueuedEvents), "MaxQueuedEvents must be at least 1.");
      }

      if (MaxConnectMetadataBytes < 0)
      {
         throw new ArgumentOutOfRangeException(nameof(MaxConnectMetadataBytes), "MaxConnectMetadataBytes must be non-negative.");
      }

      if (TickInterval <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(TickInterval), "TickInterval must be positive.");
      }

      if (HandshakeRetryInterval <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(HandshakeRetryInterval), "HandshakeRetryInterval must be positive.");
      }

      if (ConnectTimeout <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(ConnectTimeout), "ConnectTimeout must be positive.");
      }

      if (KeepAliveInterval <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(KeepAliveInterval), "KeepAliveInterval must be positive.");
      }

      if (PeerTimeout <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(PeerTimeout), "PeerTimeout must be positive.");
      }

      if (MaxReliableRetryDuration <= TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(MaxReliableRetryDuration), "MaxReliableRetryDuration must be positive.");
      }

      if (TimeSyncInterval < TimeSpan.Zero)
      {
         throw new ArgumentOutOfRangeException(nameof(TimeSyncInterval), "TimeSyncInterval must be non-negative.");
      }

      if (MinRtoMs <= 0.0 || double.IsNaN(MinRtoMs) || double.IsInfinity(MinRtoMs))
      {
         throw new ArgumentOutOfRangeException(nameof(MinRtoMs), "MinRtoMs must be positive and finite.");
      }

      if (MaxRtoMs < MinRtoMs || double.IsNaN(MaxRtoMs) || double.IsInfinity(MaxRtoMs))
      {
         throw new ArgumentOutOfRangeException(nameof(MaxRtoMs), "MaxRtoMs must be greater than or equal to MinRtoMs.");
      }

      if (MaxBytesPerSecondPerPeer is <= 0)
      {
         throw new ArgumentOutOfRangeException(nameof(MaxBytesPerSecondPerPeer), "MaxBytesPerSecondPerPeer must be null or positive.");
      }

      if (PacingBurstBytes < 0)
      {
         throw new ArgumentOutOfRangeException(nameof(PacingBurstBytes), "PacingBurstBytes must be non-negative.");
      }

      if (MaxDatagramsPerTick < 1)
      {
         throw new ArgumentOutOfRangeException(nameof(MaxDatagramsPerTick), "MaxDatagramsPerTick must be at least 1.");
      }

      IReadOnlyList<DatagramChannelConfig> configuredChannels = Channels.Count > 0
         ? Channels
         : [new DatagramChannelConfig { ChannelId = 0, DeliveryMode = DatagramDeliveryMode.ReliableOrdered }];

      var table = new DatagramChannelConfig?[64];
      var list = new List<DatagramChannelConfig>(configuredChannels.Count);

      foreach (var channel in configuredChannels)
      {
         if (channel.ChannelId >= 64)
         {
            throw new ArgumentOutOfRangeException(nameof(channel.ChannelId), "ChannelId must be in range 0..63.");
         }

         if (channel.WindowSize is < 1 or > 16384)
         {
            throw new ArgumentOutOfRangeException(nameof(channel.WindowSize), "WindowSize must be in range 1..16384.");
         }

         if (channel.MaxSendQueueDepth < 1)
         {
            throw new ArgumentOutOfRangeException(nameof(channel.MaxSendQueueDepth), "MaxSendQueueDepth must be at least 1.");
         }

         if (channel.MaxFragmentCount is < 1 or > 4096)
         {
            throw new ArgumentOutOfRangeException(nameof(channel.MaxFragmentCount), "MaxFragmentCount must be in range 1..4096.");
         }

         if (channel.MaxInFlightFragmentGroups < 1)
         {
            throw new ArgumentOutOfRangeException(nameof(channel.MaxInFlightFragmentGroups), "MaxInFlightFragmentGroups must be at least 1.");
         }

         if (table[channel.ChannelId] is not null)
         {
            throw new InvalidOperationException($"Duplicate channel registration for ChannelId {channel.ChannelId}.");
         }

         table[channel.ChannelId] = channel;
         list.Add(channel);
      }

      // deterministic 64-bit FNV-1a hash of channel layout so mismatched peer channel configs fail fast
      const ulong fnvOffset = 14695981039346656037UL;
      const ulong fnvPrime = 1099511628211UL;

      var hash = fnvOffset;
      list.Sort(static (a, b) => a.ChannelId.CompareTo(b.ChannelId));
      foreach (var ch in list)
      {
         hash ^= ch.ChannelId; hash *= fnvPrime;
         hash ^= (byte)ch.DeliveryMode; hash *= fnvPrime;
         hash ^= (byte)(ch.AllowFragmentation ? 1 : 0); hash *= fnvPrime;
      }

      configHash = hash;

      // sort send order descending by Priority, then ascending by ChannelId
      list.Sort(static (a, b) =>
      {
         var p = b.Priority.CompareTo(a.Priority);
         return p != 0 ? p : a.ChannelId.CompareTo(b.ChannelId);
      });
      orderedByPriority = [.. list];

      return table;
   }
}
