using System.Buffers;
using System.Text;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Common.Models;

public sealed class MqttAuthContext
{
   /// <summary>
   /// The incoming auth packet from the server.
   /// </summary>
   public required AuthPacketResult AuthPacket { get; init; }

   /// <summary>
   /// Which user properties are sent to the server in SendResponseAsync
   /// </summary>
   public UserPropertyListBuilder ResponseUserProperties { get; set; } = new();

   /// <summary>
   /// The packet sender used to transmit packets like acknowledgments.
   /// </summary>
   public required IMqttPacketSender PacketSender { get; init; }

   internal SignalBroker? Broker { get; init; }
   internal Task<ClientConnectResult>? ConnAckTask { get; init; }
   internal Task? ReceiveTask { get; init; }
   internal Task<AuthPacketResult>? AuthTask { get; init; }

   private bool _authTaskConsumed;

   /// <summary>
   /// Send this as response to the auth request from the server.
   /// </summary>
   public Task SendResponseAsync(
      Memory<byte> data, string reasonString, CancellationToken ct = default)
   {
      var authPacket = new AuthPacket()
      {
         ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
         AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(AuthPacket.AuthenticationMethod ?? string.Empty)),
         AuthenticationDataBytes =  new ReadOnlySequence<byte>(data),
         ReasonUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(reasonString)),
         PropertiesBytes = new ReadOnlySequence<byte>(ResponseUserProperties.WrittenMemory)
      };

      return PacketSender.SendAsync(authPacket, ct);
   }

   /// <summary>
   /// Awaits the next authentication packet from the server.
   /// </summary>
   /// <param name="ct">The cancellation token.</param>
   /// <returns>The next AuthPacketResult, or null if connection completed (CONNACK received) or failed.</returns>
   public async Task<AuthPacketResult?> AwaitNextAuthPacketAsync(CancellationToken ct = default)
   {
      if (Broker is null || ConnAckTask is null || ReceiveTask is null || AuthTask is null)
      {
         throw new InvalidOperationException("Awaiting auth packets is not supported in this context.");
      }

      if (!_authTaskConsumed)
      {
         _authTaskConsumed = true;

         var completed = await Task.WhenAny(AuthTask, ConnAckTask, ReceiveTask).WaitAsync(ct);
         if (completed == AuthTask)
         {
            return await AuthTask;
         }

         return null;
      }
      else
      {
         using var iterationCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         using var authAwaiter = Broker.AddAwaitable<AuthPacketResult>(0);

         var authTask = authAwaiter.WaitOneAsync(iterationCts.Token).AsTask();

         var completed = await Task.WhenAny(authTask, ConnAckTask, ReceiveTask);
         if (completed == authTask)
         {
            return await authTask;
         }

         await iterationCts.CancelAsync();
         try
         {
            await authTask;
         }
         catch { /* ignored */ }

         return null;
      }
   }
}
