using System.Threading.Channels;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Enums;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttClientSessions
{
   private Channel<CleanupJob>? _cleanupChannel;
   private CancellationTokenSource? _cleanupCts;
   private Task? _cleanupTask;

   private readonly struct CleanupJob
   {
      public MqttServerClient? Client { get; }
      public MqttSession? PreviousSession { get; }
      public MqttSession? ExistingSession { get; }
      public MqttSession? SessionToDispose { get; }
      public bool Unsubscribe { get; }

      public CleanupJob(MqttServerClient? client, MqttSession? previousSession, MqttSession? existingSession)
      {
         Client = client;
         PreviousSession = previousSession;
         ExistingSession = existingSession;
         SessionToDispose = null;
         Unsubscribe = false;
      }

      public CleanupJob(MqttSession session, bool unsubscribe)
      {
         Client = null;
         PreviousSession = null;
         ExistingSession = null;
         SessionToDispose = session;
         Unsubscribe = unsubscribe;
      }
   }

   public void Start()
   {
      EnsureStarted();
   }

   private void EnsureStarted()
   {
      if (_cleanupTask is not null && !_cleanupTask.IsCompleted)
      {
         return;
      }

      lock (_initiateLock)
      {
         if (_cleanupTask is not null && !_cleanupTask.IsCompleted)
         {
            return;
         }

         _cleanupChannel = Channel.CreateBounded<CleanupJob>(new BoundedChannelOptions(10000)
         {
            SingleWriter = false,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
         });

         _cleanupCts = new CancellationTokenSource();
         _cleanupTask = Task.Run(() => ProcessCleanupsAsync(_cleanupChannel, _cleanupCts.Token));
      }
   }

   public async Task StopAsync()
   {
      if (_cleanupCts is not null)
      {
         await _cleanupCts.CancelAsync();
         _cleanupCts = null;
      }

      _cleanupChannel?.Writer.TryComplete();

      if (_cleanupTask is not null)
      {
         try
         {
            await _cleanupTask.ConfigureAwait(false);
         }
         catch (OperationCanceledException)
         {
            // Expected during graceful shutdown
         }
         _cleanupTask = null;
      }

      _cleanupChannel = null;
   }

   private async Task ProcessCleanupsAsync(Channel<CleanupJob> channel, CancellationToken ct)
   {
      var reader = channel.Reader;
      try
      {
         while (await reader.WaitToReadAsync(ct))
         {
            while (reader.TryRead(out var job))
            {
               if (job.Client is not null)
               {
                  try
                  {
                     await job.Client.DisconnectAsync(new DisconnectOptions()
                     {
                        ReasonCode = DisconnectReasonCode.SessionTakenOver,
                     });
                  }
                  catch (Exception ex)
                  {
                     TraceLogger.LogServerWarning("MqttClientSessions: Failed to send session taken over disconnect packet to old client. Error: {0}", ex.Message);
                  }

                  if (_server.Events.OnDisconnect.Count > 0)
                  {
                     try
                     {
                        await _server.Events.OnDisconnect.ExecuteAsync(new MqttDisconnectContext()
                        {
                           Reason = DisconnectReasonCode.SessionTakenOver,
                           ServerClient = job.Client,
                           DisconnectKind = ClientDisconnectKind.Graceful,
                           IsSessionTakenOver = true
                        }, HandlerExecutionStrategy.SequentialContinueOnError, ct);
                     }
                     catch (Exception ex)
                     {
                        TraceLogger.LogServerWarning("MqttClientSessions: Error executing OnDisconnect for taken over client. Error: {0}", ex.Message);
                     }
                  }
               }

               if (job.PreviousSession is not null)
               {
                  try
                  {
                     await job.PreviousSession.DisposeAsync();
                  }
                  catch (Exception ex)
                  {
                     TraceLogger.LogServerWarning("MqttClientSessions: Error disposing previous session. Error: {0}", ex.Message);
                  }
               }

               if (job.ExistingSession is not null)
               {
                  try
                  {
                     await job.ExistingSession.DisposeAsync();
                  }
                  catch (Exception ex)
                  {
                     TraceLogger.LogServerWarning("MqttClientSessions: Error disposing existing session. Error: {0}", ex.Message);
                  }
               }

               if (job.SessionToDispose is not null)
               {
                  try
                  {
                     if (job.Unsubscribe)
                     {
                        _server.SubscriptionRouter.UnsubscribeAll(job.SessionToDispose);
                     }
                     await job.SessionToDispose.DisposeAsync();
                  }
                  catch (Exception ex)
                  {
                     TraceLogger.LogServerWarning("MqttClientSessions: Error disposing session. Error: {0}", ex.Message);
                  }
               }
            }
         }
      }
      catch (OperationCanceledException)
      {
         // Expected shutdown
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttClientSessions: Unexpected error in background cleanup worker: {0}", ex.Message);
      }
   }
}
