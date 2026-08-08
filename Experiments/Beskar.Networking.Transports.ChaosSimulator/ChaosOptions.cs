using System;

namespace Beskar.Networking.Transports.ChaosSimulator;

public class ChaosOptions
{
   public string ProfileName { get; set; } = "Custom";

   public double ConnectFailureRate { get; set; }
   public TimeSpan MaxConnectDelay { get; set; } = TimeSpan.Zero;

   public double SessionAbruptDisconnectRate { get; set; }
   public TimeSpan SessionLifetimeMin { get; set; } = TimeSpan.Zero;
   public TimeSpan SessionLifetimeMax { get; set; } = TimeSpan.Zero;

   public double StreamOpenFailureRate { get; set; }
   public TimeSpan MaxStreamOpenDelay { get; set; } = TimeSpan.Zero;

   public double WriteLatencyRate { get; set; }
   public TimeSpan MinWriteLatency { get; set; } = TimeSpan.Zero;
   public TimeSpan MaxWriteLatency { get; set; } = TimeSpan.Zero;

   public double ReadLatencyRate { get; set; }
   public TimeSpan MinReadLatency { get; set; } = TimeSpan.Zero;
   public TimeSpan MaxReadLatency { get; set; } = TimeSpan.Zero;

   public double PacketDropRate { get; set; }
   public double DataCorruptionRate { get; set; }

   public long? MaxWriteBytesPerSecond { get; set; }
   public long? MaxReadBytesPerSecond { get; set; }

   public static readonly ChaosOptions Clean = new()
   {
      ProfileName = "Clean (No Chaos)"
   };

   public static readonly ChaosOptions Flaky = new()
   {
      ProfileName = "Flaky",
      ConnectFailureRate = 0.15,
      MaxConnectDelay = TimeSpan.FromMilliseconds(500),
      SessionAbruptDisconnectRate = 0.05,
      PacketDropRate = 0.05
   };

   public static readonly ChaosOptions Latent = new()
   {
      ProfileName = "High Latency",
      ReadLatencyRate = 1.0,
      MinReadLatency = TimeSpan.FromMilliseconds(100),
      MaxReadLatency = TimeSpan.FromMilliseconds(300),
      WriteLatencyRate = 1.0,
      MinWriteLatency = TimeSpan.FromMilliseconds(100),
      MaxWriteLatency = TimeSpan.FromMilliseconds(300)
   };

   public static readonly ChaosOptions Corrupt = new()
   {
      ProfileName = "Corrupt Link",
      DataCorruptionRate = 0.03
   };

   public static readonly ChaosOptions Throttled = new()
   {
      ProfileName = "Throttled (Slow Link)",
      MaxReadBytesPerSecond = 50 * 1024, // 50 KB/s
      MaxWriteBytesPerSecond = 50 * 1024, // 50 KB/s
      ReadLatencyRate = 0.5,
      MinReadLatency = TimeSpan.FromMilliseconds(20),
      MaxReadLatency = TimeSpan.FromMilliseconds(50)
   };

   public static readonly ChaosOptions TotalChaos = new()
   {
      ProfileName = "Total Chaos (All Profiles Simultaneously)",
      ConnectFailureRate = 0.1,
      MaxConnectDelay = TimeSpan.FromMilliseconds(800),
      SessionAbruptDisconnectRate = 0.08,
      PacketDropRate = 0.06,
      DataCorruptionRate = 0.04,
      ReadLatencyRate = 0.8,
      MinReadLatency = TimeSpan.FromMilliseconds(80),
      MaxReadLatency = TimeSpan.FromMilliseconds(250),
      WriteLatencyRate = 0.8,
      MinWriteLatency = TimeSpan.FromMilliseconds(80),
      MaxWriteLatency = TimeSpan.FromMilliseconds(250),
      MaxReadBytesPerSecond = 100 * 1024, // 100 KB/s
      MaxWriteBytesPerSecond = 100 * 1024 // 100 KB/s
   };

   public static readonly ChaosOptions ChurnAndLeak = new()
   {
      ProfileName = "Stream & Connection Churn (High Churn Memory Leak Isolation)",
      ConnectFailureRate = 0.05,
      SessionAbruptDisconnectRate = 0.5,
      SessionLifetimeMin = TimeSpan.FromMilliseconds(200),
      SessionLifetimeMax = TimeSpan.FromMilliseconds(800),
      StreamOpenFailureRate = 0.05
   };
}
