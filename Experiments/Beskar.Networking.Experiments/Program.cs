using System.Net;
using System.Text;
using System.Buffers;
using System.Linq;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Options;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
Console.WriteLine();

var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(8000)
   .UseWs(8001)
   .UseQuic(8002)
   .Build();

mqttServer.Events.OnConnectIntercept.Add(async (ctx, ct) =>
{
   var authMethod = ctx.ConnectOptions.AuthenticationMethodUtf8Bytes.ToArray();
   var expectedMethod = new byte[] { 2, 3, 4 };
   if (!authMethod.SequenceEqual(expectedMethod))
   {
      ctx.ReasonCode = ConnectReasonCode.BadAuthenticationMethod;
      ctx.ReasonString = "Unsupported authentication method";
      return;
   }

   var initialData = ctx.ConnectOptions.AuthenticationDataBytes.ToArray();
   if (!initialData.SequenceEqual(expectedMethod))
   {
      ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
      ctx.ReasonString = "Invalid initial authentication data";
      return;
   }

   // Challenge-Response Loop
   // Send a challenge to the client: AuthPacket with ContinueAuthentication.
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

   // Wait for the client's response
   TraceLogger.LogServerInfo("Server: Awaiting challenge response from client...");
   var response = await ctx.ReceiveControlPacketAsync(ct);
   if (response is not AuthPacketOptions clientAuthOptions)
   {
      ctx.ReasonCode = ConnectReasonCode.NotAuthorized;
      ctx.ReasonString = "Expected AUTH packet from client";
      return;
   }

   // Validate response
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
   ctx.ResponseAuthenticationData = "\t\t\t"u8.ToArray(); // Success data
});

var result = await mqttServer.StartAsync();
if (result.Failed) throw new InvalidOperationException(result.Error.Detail);

var mqttClient = MqttClientFactory.CreateTcp();
var cresult = await mqttClient.ConnectAsync(new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, 8000),
   AuthenticationMethodUtf8Bytes = new byte[] { 2, 3, 4 },
   AuthenticationDataBytes = new byte[] { 2, 3, 4 },
   AuthenticationHandler = new AuthHandler()
});

await mqttClient.PingAsync();

mqttClient.AddMessageReceiveHandler((ctx, ct) =>
{
   ctx.ResponseUserProperties.Add("test2", "test2");
   TraceLogger.LogInfo(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
   return ValueTask.CompletedTask;
});

var sub = new SubscribeOptionsBuilder()
   .WithTopicFilter("test/2"u8, QualityOfServiceType.AtMostOnce, noLocal: true)
   .WithTopicFilter("teaaast/+/b"u8, QualityOfServiceType.AtMostOnce, noLocal: true)
   .WithTopicFilter("test/#"u8, QualityOfServiceType.ExactlyOnce, noLocal: true)
   .WithTopicFilter("tessadsat/#"u8, QualityOfServiceType.ExactlyOnce, noLocal: true)
   .WithUserProperty("test", "test")
   .Build();

var subAck = await mqttClient.SubscribeAsync(sub);

var pub = new PublishOptionsBuilder()
   .WithTopic("test/ssss/b"u8)
   .WithUserProperty("test1", "test1sadsadsadsadsadsadsadsadsadsadsadsadsadsadsa")
   .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
   .WithPayload("BOBA")
   .Build();

var pubRes = await mqttClient.PublishAsync(pub);

while (true) await Task.Delay(TimeSpan.FromHours(24));

return;

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
            TraceLogger.LogClientInfo($"Client: Received challenge of length {challengeBytes.Length}");
            // Transform: add 1 to each byte
            var responseBytes = new byte[challengeBytes.Length];
            for (var i = 0; i < challengeBytes.Length; i++)
            {
               responseBytes[i] = (byte)(challengeBytes[i] + 1);
            }

            TraceLogger.LogClientInfo("Client: Sending challenge response...");
            await context.SendResponseAsync(responseBytes, "Solving challenge", ct);
         }
      }
      else
      {
         TraceLogger.LogClientInfo($"Client: Received auth packet with reason code: {authPacket.ReasonCode}");
      }

      return true;
   }
}
