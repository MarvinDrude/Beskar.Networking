using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttAuthenticationTests
{
   [Test]
   public async Task ChallengeResponse_Success_ShouldConnect()
   {

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnConnectIntercept.Add(async ValueTask (ctx, ct) =>
      {
         var authMethod = ctx.ConnectOptions.AuthenticationMethodUtf8Bytes.ToArray();
         var expectedMethod = "TestAuth"u8.ToArray();
         if (!authMethod.SequenceEqual(expectedMethod))
         {
            ctx.ReasonCode = ConnectReasonCode.BadAuthenticationMethod;
            return;
         }

         var challengeBytes = new byte[] { 1, 2, 3 };
         var challengePacket = new AuthPacket
         {
            ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
            AuthenticationMethodUtf8Bytes =
               new ReadOnlySequence<byte>(ctx.ConnectOptions.AuthenticationMethodUtf8Bytes),
            AuthenticationDataBytes = new ReadOnlySequence<byte>(challengeBytes)
         };

         await ctx.SendAuthPacketAsync(new AuthPacketOptions(challengePacket), ct);

         var response = await ctx.ReceiveControlPacketAsync(ct);
         if (response is not AuthPacketOptions clientAuthOptions)
         {
            ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
            return;
         }

         var clientResponseData = clientAuthOptions.AuthenticationDataBytes.ToArray();
         var expectedResponse = new byte[] { 2, 3, 4 }; // challenge bytes + 1
         if (!clientResponseData.SequenceEqual(expectedResponse))
         {
            ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
            return;
         }

         ctx.ReasonCode = ConnectReasonCode.Success;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create and connect client
      var client = MqttClientFactory.CreateTcp();
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         AuthenticationMethodUtf8Bytes = "TestAuth"u8.ToArray(),
         AuthenticationDataBytes = "Initial"u8.ToArray(),
         AuthenticationHandler = new SuccessAuthHandler()
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      await client.PingAsync();
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task ChallengeResponse_WrongResponse_ShouldFailToConnect()
   {

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnConnectIntercept.Add(async ValueTask (ctx, ct) =>
      {
         var challengeBytes = new byte[] { 1, 2, 3 };
         var challengePacket = new AuthPacket
         {
            ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
            AuthenticationMethodUtf8Bytes =
               new ReadOnlySequence<byte>(ctx.ConnectOptions.AuthenticationMethodUtf8Bytes),
            AuthenticationDataBytes = new ReadOnlySequence<byte>(challengeBytes)
         };

         await ctx.SendAuthPacketAsync(new AuthPacketOptions(challengePacket), ct);

         var response = await ctx.ReceiveControlPacketAsync(ct);
         if (response is not AuthPacketOptions clientAuthOptions)
         {
            ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
            return;
         }

         var clientResponseData = clientAuthOptions.AuthenticationDataBytes.ToArray();
         var expectedResponse = new byte[] { 2, 3, 4 };
         if (!clientResponseData.SequenceEqual(expectedResponse))
         {
            ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
            return;
         }

         ctx.ReasonCode = ConnectReasonCode.Success;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create and connect client with WrongAuthHandler (sends back unchanged bytes instead of adding 1)
      var client = MqttClientFactory.CreateTcp();
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         AuthenticationMethodUtf8Bytes = "TestAuth"u8.ToArray(),
         AuthenticationDataBytes = "Initial"u8.ToArray(),
         AuthenticationHandler = new WrongAuthHandler()
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsTrue();
   }

   [Test]
   public async Task ChallengeResponse_UnsupportedMethod_ShouldFailToConnect()
   {

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnConnectIntercept.Add((ctx, ct) =>
      {
         var authMethod = ctx.ConnectOptions.AuthenticationMethodUtf8Bytes.ToArray();
         var expectedMethod = "SupportedMethod"u8.ToArray();
         if (!authMethod.SequenceEqual(expectedMethod))
         {
            ctx.ReasonCode = ConnectReasonCode.BadAuthenticationMethod;
            return;
         }

         ctx.ReasonCode = ConnectReasonCode.Success;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create and connect client with unsupported method name "UnsupportedMethod"
      var client = MqttClientFactory.CreateTcp();
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         AuthenticationMethodUtf8Bytes = "UnsupportedMethod"u8.ToArray(),
         AuthenticationDataBytes = "Initial"u8.ToArray()
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsTrue();
      await Assert.That(connectResult.Error.Detail).Contains("BadAuthenticationMethod");
   }

   private sealed class SuccessAuthHandler : IMqttAuthenticationHandler
   {
      public async Task<VoidResult<StringError>> ExecuteAsync(MqttAuthContext context, CancellationToken ct = default)
      {
         var authPacket = context.AuthPacket;
         if (authPacket.ReasonCode == AuthenticateReasonCode.ContinueAuthentication)
         {
            var challengeBytes = authPacket.AuthenticationData?.ToArray();
            if (challengeBytes is not null)
            {
               var responseBytes = new byte[challengeBytes.Length];
               for (var i = 0; i < challengeBytes.Length; i++) responseBytes[i] = (byte)(challengeBytes[i] + 1);
               await context.SendResponseAsync(responseBytes, "Success response", ct);
            }
         }

         return true;
      }
   }

   private sealed class WrongAuthHandler : IMqttAuthenticationHandler
   {
      public async Task<VoidResult<StringError>> ExecuteAsync(MqttAuthContext context, CancellationToken ct = default)
      {
         var authPacket = context.AuthPacket;
         if (authPacket.ReasonCode == AuthenticateReasonCode.ContinueAuthentication)
         {
            var challengeBytes = authPacket.AuthenticationData?.ToArray();
            if (challengeBytes is not null)
               // Wrong response: sends back unmodified challenge bytes
               await context.SendResponseAsync(challengeBytes, "Wrong response", ct);
         }

         return true;
      }
   }

   [Test]
   public async Task MqttV3_ConnectWithValidCredentials_ShouldConnectSuccessfully()
   {

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnConnectIntercept.Add((ctx, ct) =>
      {
         // Verify protocol version is V3.1.1
         if (ctx.ConnectOptions.ProtocolVersion != MqttProtocolVersion.V311)
         {
            ctx.ReasonCode = ConnectReasonCode.UnsupportedProtocolVersion;
            return ValueTask.CompletedTask;
         }

         var username = Encoding.UTF8.GetString(ctx.ConnectOptions.UsernameUtf8Bytes.Span);
         var password = Encoding.UTF8.GetString(ctx.ConnectOptions.PasswordBytes.Span);
         TraceLogger.LogServerInfo($"SERVER INTERCEPT V3 SUCCESS: User={username}, Pass={password}");

         if (username != "validUser" || password != "validPass")
         {
            ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
            return ValueTask.CompletedTask;
         }

         ctx.ReasonCode = ConnectReasonCode.Success;
         return ValueTask.CompletedTask;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create and connect client using MQTT v3.1.1
      var client = MqttClientFactory.CreateTcp();
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V311,
         UsernameUtf8Bytes = "validUser"u8.ToArray(),
         PasswordBytes = "validPass"u8.ToArray()
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      await client.PingAsync();
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task MqttV3_ConnectWithInvalidCredentials_ShouldFailToConnect()
   {

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      server.Events.OnConnectIntercept.Add((ctx, ct) =>
      {
         var username = Encoding.UTF8.GetString(ctx.ConnectOptions.UsernameUtf8Bytes.Span);
         var password = Encoding.UTF8.GetString(ctx.ConnectOptions.PasswordBytes.Span);
         TraceLogger.LogServerInfo($"SERVER INTERCEPT V3 FAIL: User={username}, Pass={password}");

         if (username != "validUser" || password != "validPass")
         {
            ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
            return ValueTask.CompletedTask;
         }

         ctx.ReasonCode = ConnectReasonCode.Success;
         return ValueTask.CompletedTask;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create and connect client using MQTT v3.1.1 with wrong credentials
      var client = MqttClientFactory.CreateTcp();
      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         ProtocolVersion = MqttProtocolVersion.V311,
         UsernameUtf8Bytes = "wrongUser"u8.ToArray(),
         PasswordBytes = "wrongPass"u8.ToArray()
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsTrue();
   }
}
