using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Client.Contexts;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Resilient.Server.Contexts;

namespace Beskar.Networking.Protocol.Tests;

public class ResilientAuthHandshakeTests
{
   private static async Task<bool> SpinWaitUntilAsync(Func<bool> condition, TimeSpan timeout)
   {
      using var cts = new CancellationTokenSource(timeout);
      while (!cts.IsCancellationRequested)
      {
         if (condition()) return true;
         await Task.Delay(10, cts.Token);
      }

      return condition();
   }

   [Test]
   public async Task Tcp_Port0_Auth_SuccessfulChallengeResponse_Handshake()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      server.Events.OnConnect.Add(HandleServerConnect1);

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
      await Assert.That(boundEndPoint.Port).IsGreaterThan(0);

      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      };

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);
      client.Events.OnAuthenticate.Add(HandleClientAuth1);

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var connectResult = await client.ConnectAsync(boundEndPoint, cts.Token);

      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(server.Clients.Count).IsEqualTo(1);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   private static async ValueTask HandleServerConnect1(ResilientClientConnectContext<BeskarPacket> ctx,
      CancellationToken ct)
   {
      var challengePayload = new AuthenticatePacketPayload
      {
         AuthMethod = "HMAC-SHA256",
         AuthData = Encoding.UTF8.GetBytes("challenge-nonce-100")
      };
      await ctx.SendAuthenticateAsync(challengePayload, ct);

      var responsePayload = await ctx.ReceiveAuthenticateAsync(ct);
      if (responsePayload == null ||
          Encoding.UTF8.GetString(responsePayload.AuthData) != "valid-signature-100") ctx.Deny();
   }

   private static async ValueTask HandleClientAuth1(ResilientClientAuthenticateContext<BeskarPacket> ctx,
      CancellationToken ct)
   {
      if (ctx.ChallengePayload.AuthMethod == "HMAC-SHA256" &&
          Encoding.UTF8.GetString(ctx.ChallengePayload.AuthData) == "challenge-nonce-100")
      {
         var responsePayload = new AuthenticatePacketPayload
         {
            AuthMethod = "HMAC-SHA256",
            AuthData = Encoding.UTF8.GetBytes("valid-signature-100")
         };
         await ctx.SendAuthenticateResponseAsync(responsePayload, ct);
      }
   }

   [Test]
   public async Task Tcp_Port0_Auth_DeniedByServer_Handshake()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      server.Events.OnConnect.Add(HandleServerConnectDenied);

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      };

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);
      client.Events.OnAuthenticate.Add(HandleClientAuthDenied);

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var connectResult = await client.ConnectAsync(boundEndPoint, cts.Token);

      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();

      var serverCleanedUp = await SpinWaitUntilAsync(() => server.Clients.Count == 0, TimeSpan.FromSeconds(6));
      await Assert.That(serverCleanedUp).IsTrue();

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   private static async ValueTask HandleServerConnectDenied(ResilientClientConnectContext<BeskarPacket> ctx,
      CancellationToken ct)
   {
      var challengePayload = new AuthenticatePacketPayload
      {
         AuthMethod = "HMAC-SHA256",
         AuthData = "challenge-nonce-200"u8.ToArray()
      };
      await ctx.SendAuthenticateAsync(challengePayload, ct);

      var responsePayload = await ctx.ReceiveAuthenticateAsync(ct);
      if (responsePayload == null ||
          Encoding.UTF8.GetString(responsePayload.AuthData) != "valid-signature-200") ctx.Deny();
   }

   private static async ValueTask HandleClientAuthDenied(ResilientClientAuthenticateContext<BeskarPacket> ctx,
      CancellationToken ct)
   {
      var responsePayload = new AuthenticatePacketPayload
      {
         AuthMethod = "HMAC-SHA256",
         AuthData = "invalid-signature-bad"u8.ToArray()
      };
      await ctx.SendAuthenticateResponseAsync(responsePayload, ct);
   }

   [Test]
   public async Task Tcp_Port0_Auth_PreHandshake_Denied_Handshake()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      server.Events.OnPreHandshake.Add((ctx, _) =>
      {
         ctx.Deny();
         return ValueTask.CompletedTask;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      };

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var connectResult = await client.ConnectAsync(boundEndPoint, cts.Token);

      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();

      var serverCleanedUp = await SpinWaitUntilAsync(() => server.Clients.Count == 0, TimeSpan.FromSeconds(2));
      await Assert.That(serverCleanedUp).IsTrue();

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task Tcp_Port0_Auth_MultiStep_ChallengeResponse_Handshake()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      server.Events.OnConnect.Add(HandleServerConnectMultiStep);

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;
      var clientOptions = new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      };

      var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);
      client.Events.OnAuthenticate.Add(HandleClientAuthMultiStep);

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
      var connectResult = await client.ConnectAsync(boundEndPoint, cts.Token);

      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(server.Clients.Count).IsEqualTo(1);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   private static async ValueTask HandleServerConnectMultiStep(ResilientClientConnectContext<BeskarPacket> ctx,
      CancellationToken ct)
   {
      // Step 1 challenge
      await ctx.SendAuthenticateAsync(new AuthenticatePacketPayload
      {
         AuthMethod = "SCRAM-SHA-256",
         AuthData = "step-1-nonce"u8.ToArray()
      }, ct);

      var resp1 = await ctx.ReceiveAuthenticateAsync(ct);
      if (resp1 == null || Encoding.UTF8.GetString(resp1.AuthData) != "step-1-proof")
      {
         ctx.Deny();
         return;
      }

      // Step 2 challenge
      await ctx.SendAuthenticateAsync(new AuthenticatePacketPayload
      {
         AuthMethod = "SCRAM-SHA-256",
         AuthData = "step-2-final-verifier"u8.ToArray()
      }, ct);

      var resp2 = await ctx.ReceiveAuthenticateAsync(ct);
      if (resp2 == null || Encoding.UTF8.GetString(resp2.AuthData) != "step-2-ack") ctx.Deny();
   }

   private static async ValueTask HandleClientAuthMultiStep(ResilientClientAuthenticateContext<BeskarPacket> ctx,
      CancellationToken ct)
   {
      var authDataStr = Encoding.UTF8.GetString(ctx.ChallengePayload.AuthData);
      if (authDataStr == "step-1-nonce")
         await ctx.SendAuthenticateResponseAsync(new AuthenticatePacketPayload
         {
            AuthMethod = "SCRAM-SHA-256",
            AuthData = "step-1-proof"u8.ToArray()
         }, ct);
      else if (authDataStr == "step-2-final-verifier")
         await ctx.SendAuthenticateResponseAsync(new AuthenticatePacketPayload
         {
            AuthMethod = "SCRAM-SHA-256",
            AuthData = "step-2-ack"u8.ToArray()
         }, ct);
   }

   [Test]
   public async Task Server_FrameReceived_ShouldNotFire_IfClientIsDeniedOnConnect()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>()
         .UseTcp(listenerEndPoint)
         .Build();

      var frameReceivedFired = false;
      server.Events.FrameReceived.Add((_, _) =>
      {
         frameReceivedFired = true;
         return ValueTask.CompletedTask;
      });

      server.Events.OnConnect.Add(async (ctx, _) =>
      {
         await Task.Delay(50);
         ctx.Deny();
      });

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var socket = new System.Net.Sockets.Socket(System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      await socket.ConnectAsync(boundEndPoint);

      using var writer = new Beskar.Networking.Protocol.Utilities.PooledBufferWriter();
      var connectPayload = new ConnectPacketPayload();
      var len = connectPayload.GetEncodedLength();
      if (connectPayload.TryWrite(writer.GetSpan(len), out var bytesWritten))
      {
         writer.Advance(bytesWritten);
      }

      var connectFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Connect, new ReadOnlySequence<byte>(writer.WrittenMemory));
      var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>("HelloUnauthenticated"u8.ToArray()));

      using var streamWriter = new Beskar.Networking.Protocol.Utilities.PooledBufferWriter();
      connectFrame.WriteTo(streamWriter);
      msgFrame.WriteTo(streamWriter);

      await socket.SendAsync(streamWriter.WrittenMemory);

      var serverCleanedUp = await SpinWaitUntilAsync(() => server.Clients.Count == 0, TimeSpan.FromSeconds(3));
      await Assert.That(serverCleanedUp).IsTrue();

      await Assert.That(frameReceivedFired).IsFalse();

      socket.Dispose();
      await server.StopAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task Server_HandshakeTimeout_ShouldDisconnectClient_WhenNoConnectPayloadSent()
   {
      var listenerEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var serverOptions = new ResilientServerOptions
      {
         HandshakeTimeout = TimeSpan.FromMilliseconds(300)
      };

      var server = ResilientServerFactory.CreateBuilder<BeskarPacket>(serverOptions)
         .UseTcp(listenerEndPoint)
         .Build();

      await server.StartAsync();
      var boundEndPoint = (IPEndPoint)server.Listeners.First().LocalAddress!;

      var socket = new System.Net.Sockets.Socket(System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
      await socket.ConnectAsync(boundEndPoint);

      // Client connects but sends NO payload. Handshake should timeout on server.
      var serverCleanedUp = await SpinWaitUntilAsync(() => server.Clients.Count == 0, TimeSpan.FromSeconds(2));
      await Assert.That(serverCleanedUp).IsTrue();

      socket.Dispose();
      await server.StopAsync();
      await server.DisposeAsync();
   }
}
