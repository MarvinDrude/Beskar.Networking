using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Collections;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession
{
   internal MqttWillMessageState? PendingWillMessage { get; set; }
}

internal sealed class MqttWillMessageState(
   byte[] clientId,
   string topic,
   byte[] payload,
   QualityOfServiceType qos,
   bool retain,
   uint messageExpiryInterval,
   PayloadFormat payloadFormat,
   string? contentType,
   string? responseTopic,
   byte[]? correlationData,
   UserPropertyCollection userProperties,
   uint willDelayInterval)
{
   public byte[] ClientId { get; } = clientId;

   public string Topic { get; } = topic;
   public byte[] Payload { get; } = payload;
   public QualityOfServiceType QualityOfService { get; } = qos;

   public bool Retain { get; } = retain;
   public uint MessageExpiryInterval { get; } = messageExpiryInterval;
   public PayloadFormat PayloadFormat { get; } = payloadFormat;

   public string? ContentType { get; } = contentType;
   public string? ResponseTopic { get; } = responseTopic;
   public byte[]? CorrelationData { get; } = correlationData;

   public UserPropertyCollection UserProperties { get; } = userProperties;
   public uint WillDelayInterval { get; } = willDelayInterval;

   private int _publishedOrCancelled;
   private CancellationTokenSource? _delayCts;

   public void StartDelayTimer(MqttServer server, MqttClientSessions clientSessions)
   {
      if (WillDelayInterval == 0)
      {
         TryPublish(server, clientSessions);
         return;
      }

      _delayCts = new CancellationTokenSource();
      var token = _delayCts.Token;

      _ = Task.Run(async () =>
      {
         try
         {
            await Task.Delay(TimeSpan.FromSeconds(WillDelayInterval), token);
            if (!token.IsCancellationRequested)
            {
               TryPublish(server, clientSessions);
            }
         }
         catch (OperationCanceledException)
         {
            // Ignored
         }
         catch (Exception)
         {
            // Ignored
         }
         finally
         {
            _delayCts?.Dispose();
         }
      }, token);
   }

   public bool TryPublish(MqttServer server, MqttClientSessions clientSessions)
   {
      if (Interlocked.CompareExchange(ref _publishedOrCancelled, 1, 0) != 0)
         return false;

      try
      {
         _delayCts?.Cancel();
      }
      catch (ObjectDisposedException)
      {
         // Ignored
      }

      clientSessions.RemovePendingWillMessage(ClientId);

      _ = Task.Run(async () =>
      {
         try
         {
            await server.PublishWillMessageAsync(
               Encoding.UTF8.GetString(ClientId),
               Topic,
               Payload,
               QualityOfService,
               Retain,
               MessageExpiryInterval,
               PayloadFormat,
               ContentType,
               ResponseTopic,
               CorrelationData,
               UserProperties);
         }
         catch (Exception)
         {
            /* ignored */
         }
      });
      return true;
   }

   public void Cancel()
   {
      Interlocked.Exchange(ref _publishedOrCancelled, 1);
      try
      {
         _delayCts?.Cancel();
      }
      catch (ObjectDisposedException)
      {
         // Ignored
      }
   }
}
