using Beskar.Mqtt.Client.States;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
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
