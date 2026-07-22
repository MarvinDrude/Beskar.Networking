using Beskar.Memory.Threading;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   public async Task DisconnectAsync(DisconnectOptions options, CancellationToken ct = default)
   {
      var validateResult = ValidateClient();
      if (validateResult.Failed) return;

      var beforeConnected = IsConnected;
      TraceLogger.LogClientInfo("MqttClient.DisconnectAsync: Initiating disconnect (ReasonCode: {0}, SessionExpiryInterval: {1}).",
         options.ReasonCode, options.SessionExpiryInterval);

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
         await stream.Transport.Output.CompleteAsync();
      }
      finally
      {
         await DisconnectRoutineAsync(beforeConnected, awaitReceiveTask: true);
         _gracefulDisconnect = false;
      }
   }

   internal async Task DisconnectFromReceiveLoopAsync(DisconnectOptions options, CancellationToken ct = default)
   {
      var validateResult = ValidateClient();
      if (validateResult.Failed) return;

      var beforeConnected = IsConnected;
      TraceLogger.LogClientInfo("MqttClient.DisconnectFromReceiveLoopAsync: Initiating disconnect from receive loop (ReasonCode: {0}, SessionExpiryInterval: {1}).",
         options.ReasonCode, options.SessionExpiryInterval);

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
         await stream.Transport.Output.CompleteAsync();
      }
      finally
      {
         await DisconnectRoutineAsync(beforeConnected, awaitReceiveTask: false);
         _gracefulDisconnect = false;
      }
   }

   internal void UpdateDisconnectPacket(in DisconnectPacket packet)
   {
      _disconnectReason = new MqttClientDisconnectReason(true, (int)packet.ReasonCode);
      _disconnectUserProperties = UserPropertyCollection.Create(packet.PropertiesBytes);

      if (!packet.ReasonUtf8Bytes.IsEmpty)
      {
         _disconnectReasonString = packet.ReasonUtf8Bytes.GetUtf8String();
      }
   }

    private ValueTask DisconnectInternalAsync(bool awaitReceiveTask = true, bool awaitKeepAliveTask = true)
    {
       var beforeConnected = IsConnected;

       // Only disconnect if not already in progress
       return DisconnectingAlreadyInProcessOrDone()
          ? ValueTask.CompletedTask
          : DisconnectRoutineAsync(beforeConnected, awaitReceiveTask, awaitKeepAliveTask);
    }

    private async ValueTask DisconnectRoutineAsync(bool beforeConnected, bool awaitReceiveTask = true, bool awaitKeepAliveTask = true)
    {
       TraceLogger.LogClientInfo("MqttClient: Starting disconnect routine (BeforeConnected: {0}).", beforeConnected);
       await _clientTokenSource.CancelAsync();

       try
       {
          await _networkClient.DisconnectAsync();
       }
       catch (Exception ex)
       {
          TraceLogger.LogClientError("MqttClient: Error disconnecting inner network client: {0}", ex.Message);
       }

       try
       {
          var task = _keepAliveTask;
          if (awaitKeepAliveTask && task is not null)
          {
             await task;
          }
       }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("MqttClient: Error waiting for keep-alive task to end: {0}", ex.Message);
      }

      try
      {
         if (awaitReceiveTask)
         {
            var task = _receiveTask;
            if (task is not null)
            {
               await task;
            }
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("MqttClient: Error waiting for receive task to end: {0}", ex.Message);
      }

      try
      {
         var stream = Interlocked.Exchange(ref _controlStream, null);
         if (stream is not null)
         {
            await stream.DisposeAsync();
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("MqttClient: Error disposing control stream: {0}", ex.Message);
      }

      try
      {
         var session = Interlocked.Exchange(ref _networkSession, null);
         if (session is not null)
         {
            await session.DisposeAsync();
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("MqttClient: Error disposing network session: {0}", ex.Message);
      }
      finally
      {
         lock (_topicAliasesLock)
         {
            _topicAliases.Clear();
         }

         try
         {
            var sem = _inFlightSemaphore;
            _inFlightSemaphore = null;

            sem?.Dispose();
         }
         catch
         {
            // ignored
         }

         CompareExchangeState(MqttClientConnectionState.Disconnected, MqttClientConnectionState.Disconnecting);
         TraceLogger.LogClientInfo("MqttClient: Disconnected from transport layer. State transitioned to Disconnected.");

         if (Events.OnClientDisconnected.Count > 0)
         {
            var clientDisconnectedContext = new ClientDisconnectedContext()
            {
               BeforeConnected = beforeConnected,
               ReasonCode = _disconnectReason.HasValue
                  ? (DisconnectReasonCode)_disconnectReason.Value.ReasonCode : DisconnectReasonCode.NormalDisconnection,
               Exception = _disconnectException,
               UserProperties = _disconnectUserProperties,
               ReasonString = _disconnectReasonString,
            };

            _ = Task.Run(() => Events.OnClientDisconnected.ExecuteAsync(
               clientDisconnectedContext, HandlerExecutionStrategy.SequentialContinueOnError));
         }
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
            TraceLogger.LogClientInfo("MqttClient: Disconnect already in progress or completed (Current state: {0}).", status);
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
