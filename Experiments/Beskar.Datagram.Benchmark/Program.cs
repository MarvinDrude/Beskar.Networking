using System.Net;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Datagram.Benchmark;

public static class Program
{
   public static void Main(string[] args)
   {
      BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
   }
}

[MemoryDiagnoser]
public class DatagramChannelBenchmarks
{
   private SimulatedNetworkHub _hub = null!;
   private DatagramHost _server = null!;
   private DatagramHost _client = null!;
   private DatagramPeer _clientPeer = null!;
   private byte[] _payload = null!;

   [Params(DatagramSecurityMode.Crc32C, DatagramSecurityMode.Aead)]
   public DatagramSecurityMode SecurityMode { get; set; }

   [GlobalSetup]
   public void Setup()
   {
      _hub = new SimulatedNetworkHub(seed: 1);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9500);

      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = SecurityMode }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, windowSize: 512, maxSendQueueDepth: 2048)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);

      _server = new DatagramHost(serverEp, MakeOpts(), _hub.CreateTransport(), _hub.Time);
      _client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), _hub.CreateTransport(), _hub.Time);
      _server.Start();
      _client.Start();

      var task = _client.ConnectAsync(serverEp);
      for (var i = 0; i < 30 && !task.IsCompleted; i++)
      {
         _client.Tick();
         _hub.DeliverDue();
         _server.Tick();
         _hub.DeliverDue();
      }

      _clientPeer = task.GetAwaiter().GetResult();
      _server.PollEvents(_ => { });
      _client.PollEvents(_ => { });

      _payload = new byte[64];
      new Random(42).NextBytes(_payload);
   }

   [Benchmark]
   public int Burst_32UnreliableSequenced_CoalescedRoundTrip()
   {
      for (var i = 0; i < 32; i++)
      {
         _clientPeer.TrySend(1, _payload);
      }

      _client.Tick();
      _hub.DeliverDue();
      _server.Tick();
      _hub.DeliverDue();

      var count = 0;
      _server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived) count++;
      });
      _client.PollEvents(_ => { });
      return count;
   }

   [Benchmark]
   public int Burst_32ReliableOrdered_CoalescedAndAcked()
   {
      for (var i = 0; i < 32; i++)
      {
         _clientPeer.TrySend(0, _payload);
      }

      _client.Tick();
      _hub.DeliverDue();
      _server.Tick();
      _hub.DeliverDue();
      _client.Tick();
      _hub.DeliverDue();

      var count = 0;
      _server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived) count++;
      });
      _client.PollEvents(_ => { });
      return count;
   }

   [GlobalCleanup]
   public void Cleanup()
   {
      _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
      _server.DisposeAsync().AsTask().GetAwaiter().GetResult();
   }
}
