using System.Net;
using System.Text;
using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Common.Options;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Authentication Example (v3.1.1 and v5.0 Challenges)");
Console.WriteLine("==========================================================");

// Build and configure the MQTT server
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(8000)
   .Build();

// Intercept and handle incoming connect requests
mqttServer.Events.OnConnectIntercept.Add(async ValueTask (ctx, ct) =>
{
   var protocolVersion = ctx.ConnectOptions.ProtocolVersion;
   TraceLogger.LogServerInfo($"Server: Received connect request for protocol: {protocolVersion}");

   if (protocolVersion is MqttProtocolVersion.V311 or MqttProtocolVersion.V31)
   {
      // --- MQTT v3 Simple Username/Password Authentication ---
      var username = ctx.ConnectOptions.UsernameUtf8Bytes.IsEmpty
         ? string.Empty
         : Encoding.UTF8.GetString(ctx.ConnectOptions.UsernameUtf8Bytes.Span);

      var password = ctx.ConnectOptions.PasswordBytes.IsEmpty
         ? string.Empty
         : Encoding.UTF8.GetString(ctx.ConnectOptions.PasswordBytes.Span);

      TraceLogger.LogServerInfo($"Server: Received v3 connect request for User: '{username}'");

      // Validate credentials (expect "admin" and "secret")
      if (username == "admin" && password == "secret")
      {
         TraceLogger.LogServerInfo("Server: MQTT v3 Username/Password validation SUCCESS!");
         ctx.ReasonCode = ConnectReasonCode.Success;
      }
      else
      {
         TraceLogger.LogServerWarning("Server: MQTT v3 Username/Password validation FAILED!");
         ctx.ReasonCode = ConnectReasonCode.BadUserNameOrPassword;
         ctx.ReasonString = "Invalid username or password";
      }
   }
   else if (protocolVersion == MqttProtocolVersion.V50)
   {
      // --- MQTT v5 Enhanced Authentication (Challenge-Response) ---
      var authMethod = ctx.ConnectOptions.AuthenticationMethodUtf8Bytes.ToArray();
      var expectedMethod = "ChallengeResponse"u8.ToArray();

      if (!authMethod.SequenceEqual(expectedMethod))
      {
         TraceLogger.LogServerWarning("Server: Unsupported authentication method.");

         ctx.ReasonCode = ConnectReasonCode.BadAuthenticationMethod;
         ctx.ReasonString = "Unsupported authentication method. Expected 'ChallengeResponse'";
         return;
      }

      var initialData = ctx.ConnectOptions.AuthenticationDataBytes.ToArray();
      var expectedInitialData = new byte[] { 2, 3, 4 };

      if (!initialData.SequenceEqual(expectedInitialData))
      {
         TraceLogger.LogServerWarning("Server: Invalid initial authentication data.");

         ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
         ctx.ReasonString = "Invalid initial authentication data";
         return;
      }

      // Generate a challenge (e.g. random bytes) and send to the client
      var challengeBytes = new byte[] { 10, 20, 30 };
      var challengePacket = new AuthPacket
      {
         ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
         AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(ctx.ConnectOptions.AuthenticationMethodUtf8Bytes),
         AuthenticationDataBytes = new ReadOnlySequence<byte>(challengeBytes),
         ReasonUtf8Bytes = new ReadOnlySequence<byte>([.. "Challenge"u8])
      };

      TraceLogger.LogServerInfo("Server: Sending challenge to client...");
      await ctx.SendAuthPacketAsync(new AuthPacketOptions(challengePacket), ct);

      // Await client response
      TraceLogger.LogServerInfo("Server: Awaiting challenge response from client...");
      var response = await ctx.ReceiveControlPacketAsync(ct);

      if (response is not AuthPacketOptions clientAuthOptions)
      {
         ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
         ctx.ReasonString = "Expected AUTH packet from client";
         return;
      }

      // Validate that the client solved the challenge (increment each byte by 1)
      var clientResponseData = clientAuthOptions.AuthenticationDataBytes.ToArray();
      var expectedResponse = new byte[] { 11, 21, 31 }; // challengeBytes + 1

      if (!clientResponseData.SequenceEqual(expectedResponse))
      {
         TraceLogger.LogServerWarning("Server: Challenge verification FAILED!");

         ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
         ctx.ReasonString = "Invalid challenge response";
         return;
      }

      TraceLogger.LogServerInfo("Server: Challenge verification SUCCESS!");
      ctx.ReasonCode = ConnectReasonCode.Success;
      ctx.ResponseAuthenticationData = "AuthSuccess"u8.ToArray();
   }
   else
   {
      ctx.ReasonCode = ConnectReasonCode.UnsupportedProtocolVersion;
      ctx.ReasonString = "Unsupported protocol version";
   }
});

// Start the MQTT Server
TraceLogger.LogServerInfo("Server: Starting...");
var startResult = await mqttServer.StartAsync();

if (startResult.Failed)
{
   throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
}

TraceLogger.LogServerInfo("Server: Running on port 8000.");

// 4. Run Demonstration Cases
try
{
   await RunV3ClientDemo();
   await RunV5ClientDemo();
}
finally
{
   TraceLogger.LogServerInfo("Server: Stopping...");
   await mqttServer.StopAsync();
   TraceLogger.LogServerInfo("Server: Stopped.");
}

Console.WriteLine("==========================================================");
Console.WriteLine(" Demo Finished Successfully.");
Console.WriteLine("==========================================================");
return;

async Task RunV3ClientDemo()
{
   TraceLogger.LogInfo("\n--- Starting MQTT v3 Client (Username/Password Auth) ---");
   await using var mqttClient = MqttClientFactory.CreateTcp();

   // Connect with valid credentials
   TraceLogger.LogClientInfo("Client v3: Connecting with valid credentials...");
   var connResult = await mqttClient.ConnectAsync(new ConnectOptions
   {
      EndPoint = new IPEndPoint(IPAddress.Loopback, 8000),
      ProtocolVersion = MqttProtocolVersion.V311,
      UsernameUtf8Bytes = "admin"u8.ToArray(),
      PasswordBytes = "secret"u8.ToArray()
   });

   if (!connResult.Failed)
   {
      TraceLogger.LogClientInfo("Client v3: Connected successfully!");
      await mqttClient.PingAsync();

      TraceLogger.LogClientInfo("Client v3: Disconnecting...");
      await mqttClient.DisconnectAsync(new DisconnectOptions());
   }
   else
   {
      TraceLogger.LogClientWarning($"Client v3: Connection failed! {connResult.Error.Detail}");
   }

   // Connect with invalid credentials
   TraceLogger.LogClientInfo("Client v3: Connecting with invalid credentials...");
   var badConnResult = await mqttClient.ConnectAsync(new ConnectOptions
   {
      EndPoint = new IPEndPoint(IPAddress.Loopback, 8000),
      ProtocolVersion = MqttProtocolVersion.V311,
      UsernameUtf8Bytes = "bad_user"u8.ToArray(),
      PasswordBytes = "bad_pass"u8.ToArray()
   });

   if (badConnResult.Failed)
   {
      TraceLogger.LogClientInfo($"Client v3: Rejected as expected. Reason: {badConnResult.Error.Detail}");
   }
   else
   {
      TraceLogger.LogClientError("Client v3: Unexpected connection success with invalid credentials!");
      await mqttClient.DisconnectAsync(new DisconnectOptions());
   }
}

async Task RunV5ClientDemo()
{
   TraceLogger.LogInfo("\n--- Starting MQTT v5 Client (Enhanced Auth / Challenge-Response) ---");
   await using var mqttClient = MqttClientFactory.CreateTcp();

   TraceLogger.LogClientInfo("Client v5: Connecting with ChallengeResponse handler...");
   var connResult = await mqttClient.ConnectAsync(new ConnectOptions
   {
      EndPoint = new IPEndPoint(IPAddress.Loopback, 8000),
      ProtocolVersion = MqttProtocolVersion.V50,
      AuthenticationMethodUtf8Bytes = "ChallengeResponse"u8.ToArray(),
      AuthenticationDataBytes = new byte[] { 2, 3, 4 },
      AuthenticationHandler = new AuthHandler()
   });

   if (!connResult.Failed)
   {
      TraceLogger.LogClientInfo("Client v5: Connected successfully!");
      await mqttClient.PingAsync();

      TraceLogger.LogClientInfo("Client v5: Disconnecting...");
      await mqttClient.DisconnectAsync(new DisconnectOptions());
   }
   else
   {
      TraceLogger.LogClientWarning($"Client v5: Connection failed! {connResult.Error.Detail}");
   }
}

public sealed class AuthHandler : IMqttAuthenticationHandler
{
   public async Task<VoidResult<StringError>> ExecuteAsync(
      MqttAuthContext context, CancellationToken ct = default)
   {
      var authPacket = context.AuthPacket;
      if (authPacket.ReasonCode == AuthenticateReasonCode.ContinueAuthentication)
      {
         var challengeBytes = authPacket.AuthenticationData?.ToArray();
         if (challengeBytes is not null)
         {
            TraceLogger.LogClientInfo($"Client AuthHandler: Received challenge of length {challengeBytes.Length}");

            // Solve the challenge: increment each byte by 1
            var responseBytes = new byte[challengeBytes.Length];
            for (var i = 0; i < challengeBytes.Length; i++)
            {
               responseBytes[i] = (byte)(challengeBytes[i] + 1);
            }

            TraceLogger.LogClientInfo("Client AuthHandler: Sending response...");
            await context.SendResponseAsync(responseBytes, "Challenge solved", ct);
         }
      }
      else
      {
         TraceLogger.LogClientInfo($"Client AuthHandler: Authenticated. Reason code: {authPacket.ReasonCode}");
      }

      return true;
   }
}
