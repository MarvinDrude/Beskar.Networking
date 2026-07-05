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

         _gracefulDisconnect = true;
         _disconnectReason = new MqttClientDisconnectReason(true, (int)options.ReasonCode);

         await Send(options, stream, 0, ct);
      }
      finally
      {
         await DisconnectRoutineAsync(beforeConnected);
         _gracefulDisconnect = false;
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

   private async ValueTask DisconnectRoutineAsync(bool beforeConnected)
   {
      await _clientTokenSource.CancelAsync();

      try
      {
         await _networkClient.DisconnectAsync();
      }
      catch (Exception)
      {
         // ignored
      }

      try
      {
         var task = _keepAliveTask;
         if (task is not null)
         {
            await task;
         }
      }
      catch (Exception)
      {
         // ignored
      }
      finally
      {
         CompareExchangeState(MqttClientConnectionState.Disconnected, MqttClientConnectionState.Disconnecting);
      }
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
