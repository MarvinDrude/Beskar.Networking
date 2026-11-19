using System.Net;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Nat;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Networking.Datagram.Tests;

[NotInParallel]
public class NatHolePunchingTests
{
   [Test]
   public async Task RendezvousAndPunchAndConnect_EstablishesDirectEncryptedSessionOverSingleSocket()
   {
      var rendezvousOpts = new DatagramOptions { Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C } };
      await using var rendezvous = new DatagramRendezvousServer(new IPEndPoint(IPAddress.Loopback, 0), options: rendezvousOpts);
      rendezvous.Start();

      var peerOptsA = new DatagramOptions { DispatchMode = DatagramEventDispatchMode.Async }
         .AddChannel(0, DatagramDeliveryMode.ReliableOrdered);
      var peerOptsB = new DatagramOptions { DispatchMode = DatagramEventDispatchMode.Async }
         .AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      await using var hostA = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), peerOptsA);
      await using var hostB = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), peerOptsB);

      var punchIntroA = new TaskCompletionSource<DatagramPunchIntroduction>(TaskCreationOptions.RunContinuationsAsynchronously);
      var punchIntroB = new TaskCompletionSource<DatagramPunchIntroduction>(TaskCreationOptions.RunContinuationsAsynchronously);
      var msgOnB = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

      hostA.Events.PunchIntroduced = (intro, _) => { punchIntroA.TrySetResult(intro); return ValueTask.CompletedTask; };
      hostB.Events.PunchIntroduced = (intro, _) => { punchIntroB.TrySetResult(intro); return ValueTask.CompletedTask; };
      hostB.Events.MessageReceived = (_, _, _, payload, _) =>
      {
         msgOnB.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         return ValueTask.CompletedTask;
      };

      hostA.Start();
      hostB.Start();

      var nodeA = "peer-A"u8.ToArray();
      var nodeB = "peer-B"u8.ToArray();

      var pubA = await hostA.DiscoverPublicEndPointAsync(rendezvous.LocalEndPoint!, nodeA);
      var pubB = await hostB.DiscoverPublicEndPointAsync(rendezvous.LocalEndPoint!, nodeB);

      await Assert.That(pubA).IsEqualTo(hostA.LocalEndPoint);
      await Assert.That(pubB).IsEqualTo(hostB.LocalEndPoint);

      const ulong punchToken = 0xCAFE_BABE_1234_5678UL;
      hostA.RequestIntroduction(rendezvous.LocalEndPoint!, nodeA, nodeB, punchToken);

      var introA = await punchIntroA.Task.WaitAsync(TimeSpan.FromSeconds(3));
      var introB = await punchIntroB.Task.WaitAsync(TimeSpan.FromSeconds(3));

      // Both peers simultaneously punch on their bound sockets; A then connects over the punched pinhole!
      var punchTaskB = hostB.PunchAsync(introB.PeerPublicEndPoint, introB.PeerLanEndPoint, introB.PunchToken);
      var connectTaskA = hostA.PunchAndConnectAsync(introA.PeerPublicEndPoint, introA.PeerLanEndPoint, introA.PunchToken);

      var resB = await punchTaskB;
      var peerOnA = await connectTaskA;

      await Assert.That(resB.Succeeded).IsTrue();
      await Assert.That(peerOnA).IsNotNull();

      peerOnA!.TrySend(0, "p2p-punched-hello!"u8);
      var received = await msgOnB.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(received).IsEqualTo("p2p-punched-hello!");
   }

   [Test]
   public async Task PunchAsync_UnreachableTarget_SignalsRelayFallback()
   {
      await using var host = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0));
      host.Start();

      var result = await host.PunchAsync(
         new IPEndPoint(IPAddress.Loopback, 65432),
         peerLanEndPoint: null,
         punchToken: 999UL,
         options: new DatagramPunchOptions
         {
            ProbeInterval = TimeSpan.FromMilliseconds(30),
            Timeout = TimeSpan.FromMilliseconds(180)
         });

      await Assert.That(result.Succeeded).IsFalse();
      await Assert.That(result.NeedsRelayFallback).IsTrue();
      await Assert.That(result.ResolvedEndPoint).IsNull();
   }
}
