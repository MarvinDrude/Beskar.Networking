using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   public async Task DisconnectAsync(DisconnectOptions options, CancellationToken ct = default)
   {
      var validateResult = ValidateClient();
      if (validateResult.Failed) return;

      var beforeConnected = IsConnected;
      if (DisconnectingAlreadyInProcessOrDone())
      {
         return;
      }

      if (_controlStream is not { } stream)
      {
         return;
      }

      try
      {
         if (!beforeConnected)
         {
            return;
         }

         _disconnectReason = new MqttClientDisconnectReason(true, (int)options.ReasonCode);

         using (await stream.AcquireWriterLock(ct))
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).WriteDisconnect(options);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).WriteDisconnect(options);
                  break;
               default:
                  throw new InvalidOperationException("Unkown protocol version.");
            }

            await writer.FlushAsync(ct);
         }
      }
      finally
      {
         await DisconnectRoutineAsync(beforeConnected);
      }
   }

   private ValueTask DisconnectInternalAsync()
   {
      var beforeConnected = IsConnected;

      // Only disconnect if not already in progress
      return DisconnectingAlreadyInProcessOrDone()
         ? ValueTask.CompletedTask
         : DisconnectRoutineAsync(beforeConnected);
   }

   private ValueTask DisconnectRoutineAsync(bool beforeConnected)
   {
      _clientTokenSource.Cancel();

      try
      {
         if (_networkClient.)
      }
      catch (Exception)
      {
         // ignored
      }

      return ValueTask.CompletedTask;
   }

   private bool DisconnectingAlreadyInProcessOrDone()
   {
      var status = (MqttClientConnectionState)_state;

      while (true)
      {
         // Already in the desired states -> just return
         if (status is MqttClientConnectionState.Disconnected
             or MqttClientConnectionState.Disconnecting)
         {
            return true;
         }

         // Still in connected or connecting
         var currStatus = CompareExchangeState(MqttClientConnectionState.Disconnecting, status);
         if (currStatus == status)
         {
            return false;
         }

         status = currStatus;
      }
   }
}
