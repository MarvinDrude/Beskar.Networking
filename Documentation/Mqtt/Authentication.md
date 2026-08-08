# MQTT Authentication

Beskar.Mqtt provides robust authentication support for both standard MQTT v3.1.1 username/password flows
and MQTT v5.0 Enhanced Authentication (Challenge-Response) handshakes.

---

## 1. Server-Side Authentication Interceptor

To implement authentication on the server, subscribe to the `OnConnectIntercept` event.
The interceptor is triggered when a client sends its initial `CONNECT` packet.

### Example Server Setup

```csharp
using System.Net;
using System.Text;
using System.Buffers;
using System.Linq;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Common.Options;

// Build the MQTT server
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(8000)
   .Build();

// Intercept and handle incoming connect requests
mqttServer.Events.OnConnectIntercept.Add(async ValueTask (ctx, ct) =>
{
   var protocolVersion = ctx.ConnectOptions.ProtocolVersion;

   if (protocolVersion is MqttProtocolVersion.V311 or MqttProtocolVersion.V31)
   {
      // --- MQTT v3 Simple Username/Password Authentication ---
      var username = ctx.ConnectOptions.UsernameUtf8Bytes.IsEmpty
         ? string.Empty
         : Encoding.UTF8.GetString(ctx.ConnectOptions.UsernameUtf8Bytes.Span);

      var password = ctx.ConnectOptions.PasswordBytes.IsEmpty
         ? string.Empty
         : Encoding.UTF8.GetString(ctx.ConnectOptions.PasswordBytes.Span);

      // Validate credentials (e.g., admin / secret)
      if (username == "admin" && password == "secret")
      {
         ctx.ReasonCode = ConnectReasonCode.Success;
      }
      else
      {
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
         ctx.ReasonCode = ConnectReasonCode.BadAuthenticationMethod;
         ctx.ReasonString = "Unsupported authentication method. Expected 'ChallengeResponse'";
         return;
      }

      var initialData = ctx.ConnectOptions.AuthenticationDataBytes.ToArray();
      var expectedInitialData = new byte[] { 2, 3, 4 };

      if (!initialData.SequenceEqual(expectedInitialData))
      {
         ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
         ctx.ReasonString = "Invalid initial authentication data";
         return;
      }

      // Generate a challenge and send it to the client
      var challengeBytes = new byte[] { 10, 20, 30 };
      var challengePacket = new AuthPacket
      {
         ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
         AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(ctx.ConnectOptions.AuthenticationMethodUtf8Bytes),
         AuthenticationDataBytes = new ReadOnlySequence<byte>(challengeBytes),
         ReasonUtf8Bytes = new ReadOnlySequence<byte>([.. "Challenge"u8])
      };

      await ctx.SendAuthPacketAsync(new AuthPacketOptions(challengePacket), ct);

      // Await client response
      var response = await ctx.ReceiveControlPacketAsync(ct);

      if (response is not AuthPacketOptions clientAuthOptions)
      {
         ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
         ctx.ReasonString = "Expected AUTH packet from client";
         return;
      }

      // Validate that the client solved the challenge (e.g., incremented each byte by 1)
      var clientResponseData = clientAuthOptions.AuthenticationDataBytes.ToArray();
      var expectedResponse = new byte[] { 11, 21, 31 }; // challengeBytes + 1

      if (!clientResponseData.SequenceEqual(expectedResponse))
      {
         ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
         ctx.ReasonString = "Invalid challenge response";
         return;
      }

      // Finalize success and optionally provide success authentication data
      ctx.ReasonCode = ConnectReasonCode.Success;
      ctx.ResponseAuthenticationData = "AuthSuccess"u8.ToArray();
   }
   else
   {
      ctx.ReasonCode = ConnectReasonCode.UnsupportedProtocolVersion;
      ctx.ReasonString = "Unsupported protocol version";
   }
});

// Start the server
await mqttServer.StartAsync();
```

---

## 2. Client-Side Authentication

To connect to the server, configure the client using the appropriate credentials or
authentication handler based on the MQTT protocol version.

### MQTT v3.1.1 Username/Password Authentication

To authenticate using MQTT v3.1.1, specify `UsernameUtf8Bytes` and `PasswordBytes` in your `ConnectOptions`.

```csharp
using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;

await using var mqttClient = MqttClientFactory.CreateTcp();

var connResult = await mqttClient.ConnectAsync(new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, 8000),
   ProtocolVersion = MqttProtocolVersion.V311,
   UsernameUtf8Bytes = "admin"u8.ToArray(),
   PasswordBytes = "secret"u8.ToArray()
});

if (!connResult.Failed)
{
   Console.WriteLine("Connected successfully!");
   await mqttClient.DisconnectAsync(new DisconnectOptions());
}
else
{
   Console.WriteLine($"Connection failed: {connResult.Error.Detail}");
}
```

### MQTT v5.0 Enhanced Authentication (Challenge-Response)

To authenticate using MQTT v5.0 Enhanced Authentication, provide the `AuthenticationMethodUtf8Bytes`,
initial `AuthenticationDataBytes`, and register an implementation of `IMqttAuthenticationHandler`.

```csharp
using System.Net;
using System.Linq;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;

await using var mqttClient = MqttClientFactory.CreateTcp();

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
   Console.WriteLine("Connected successfully!");
   await mqttClient.DisconnectAsync(new DisconnectOptions());
}
```

#### Implementing the Authentication Handler (`IMqttAuthenticationHandler`)

The handler receives `MqttAuthContext`, which contains the incoming challenge `AuthPacket` sent
from the server. Use `context.SendResponseAsync(...)` to transmit the response back to the server.

```csharp
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
            // Solve the challenge (e.g., increment each byte by 1)
            var responseBytes = new byte[challengeBytes.Length];
            for (var i = 0; i < challengeBytes.Length; i++)
            {
               responseBytes[i] = (byte)(challengeBytes[i] + 1);
            }

            // Send response back to the server
            await context.SendResponseAsync(responseBytes, "Challenge solved", ct);
         }
      }
      else
      {
         Console.WriteLine($"Authenticated. Reason code: {authPacket.ReasonCode}");
      }

      return true;
   }
}
```

---

## 3. Server-Side Message Interception & Topic Authorization (`OnPublishIntercept`)

To enforce fine-grained topic authorization or block specific incoming published messages before they reach subscribers
or retained message stores, subscribe to the `OnPublishIntercept` event.

### Intercepting & Blocking Published Messages

```csharp
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Contexts;

// Register incoming publish interceptor
mqttServer.Events.OnPublishIntercept.Add((ctx, ct) =>
{
   var topic = ctx.PublishMessage.Topic;
   var clientId = ctx.Client.ClientId;

   // 1. Topic authorization: restrict admin topics to specific client IDs
   if (topic.StartsWith("admin/") && clientId != "authorized_admin")
   {
      // Block the message and return NotAuthorized to QoS 1/2 publishers
      ctx.Block(reasonCode: (byte)PubAckReasonCode.NotAuthorized);
      return ValueTask.CompletedTask;
   }

   // 2. Block/ignore spam or malformed topics (silent drop)
   if (topic.StartsWith("spam/"))
   {
      ctx.Block(); // Defaults to PubAckReasonCode.Success (silent drop)
      return ValueTask.CompletedTask;
   }

   return ValueTask.CompletedTask;
});
```

When `ctx.IsBlocked` or `ctx.Block()` is executed:
- The message is **ignored** and will not be dispatched to subscribers.
- The message is **not stored** in the retained messages cache.
- For QoS 1 & QoS 2 publishes, an acknowledgment (`PUBACK` / `PUBREC`) is sent back with the specified `ReasonCode` to prevent publisher client hangs.

