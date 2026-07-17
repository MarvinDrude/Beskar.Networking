using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   public async Task<Result<SubscribeResult, StringError>> SubscribeAsync(
      SubscribeOptions options, CancellationToken ct = default)
   {
      var validateResult = SubscribeOptionsValidator.Validate(options);
      if (!validateResult.IsSuccess) return validateResult.Error;

      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult.Error;

      if (_controlStream is not { } stream)
      {
         return new StringError("Invalid control stream.");
      }

      TraceLogger.LogClientInfo("MqttClient.SubscribeAsync: Subscribing to topic filters...");

      try
      {
         SubAckPacket subAck;
         if (ct.CanBeCanceled)
         {
            subAck = await SendAndAck<SubscribeOptions, SubAckPacket>(options, stream, ct);
         }
         else
         {
            using var tokenSource = new CancellationTokenSource(_connectOptions.Timeout);
            subAck = await SendAndAck<SubscribeOptions, SubAckPacket>(options, stream, tokenSource.Token);
         }

         TraceLogger.LogClientInfo("MqttClient.SubscribeAsync: Received SUBACK (PacketId: {0}).", subAck.PacketIdentifier);

         using var filterBuilder = new ArrayBuilder<MqttTopicFilter>(12);
         var filterEnumerator = options.TopicFilters.GetEnumerator();

         while (filterEnumerator.MoveNext())
         {
            filterBuilder.Add(new MqttTopicFilter(filterEnumerator.Current));
         }

         using var reasonCodeBuilder = new ArrayBuilder<SubscribeReasonCode>(12);
         var reasonCodeEnumerator = subAck.GetReturnCodes();

         while (reasonCodeEnumerator.MoveNext())
         {
            reasonCodeBuilder.Add(reasonCodeEnumerator.Current);
         }

         if (reasonCodeBuilder.Count != 0 && filterBuilder.Count != reasonCodeBuilder.Count)
         {
            return new StringError("The reason codes length is different to topic filters.");
         }

         string? reasonString = null;
         if (subAck.ReasonStringUtf8Bytes.Length > 0)
         {
            reasonString = subAck.ReasonStringUtf8Bytes.GetUtf8String();
         }

         var subscriptions = new MqttTopicSubscriptionResult[filterBuilder.Count];
         for (var i = 0; i < filterBuilder.Count; i++)
         {
            subscriptions[i] = new MqttTopicSubscriptionResult
            {
               TopicFilter = filterBuilder.WrittenSpan[i],
               ReasonCode = reasonCodeBuilder.Count > i ? reasonCodeBuilder.WrittenSpan[i] : SubscribeReasonCode.GrantedQos0
            };
         }

         return new SubscribeResult
         {
            PacketIdentifier = subAck.PacketIdentifier,
            ReasonString = reasonString,
            Subscriptions = subscriptions,
            UserProperties = UserPropertyCollection.Create(subAck.PropertiesBytes)
         };
      }

      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.SubscribeAsync: Error subscribing: {0}", error.Message);
         return new StringError(error.ToString());
      }
   }

   public async Task<Result<UnsubscribeResult, StringError>> UnsubscribeAsync(
      UnsubscribeOptions options, CancellationToken ct = default)
   {
      var validateResult = UnsubscribeOptionsValidator.Validate(options);
      if (!validateResult.IsSuccess) return validateResult.Error;

      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult.Error;

      if (_controlStream is not { } stream)
      {
         return new StringError("Invalid control stream.");
      }

      TraceLogger.LogClientInfo("MqttClient.UnsubscribeAsync: Unsubscribing from topics...");

      try
      {
         UnsubAckPacket unsubAck;
         if (ct.CanBeCanceled)
         {
            unsubAck = await SendAndAck<UnsubscribeOptions, UnsubAckPacket>(options, stream, ct);
         }
         else
         {
            using var tokenSource = new CancellationTokenSource(_connectOptions.Timeout);
            unsubAck = await SendAndAck<UnsubscribeOptions, UnsubAckPacket>(options, stream, tokenSource.Token);
         }

         TraceLogger.LogClientInfo("MqttClient.UnsubscribeAsync: Received UNSUBACK (PacketId: {0}).", unsubAck.PacketIdentifier);

         using var filterBuilder = new ArrayBuilder<string>(12);
         var filterEnumerator = options.TopicFilters.GetEnumerator();

         while (filterEnumerator.MoveNext())
         {
            filterBuilder.Add(System.Text.Encoding.UTF8.GetString(filterEnumerator.Current));
         }

         using var reasonCodeBuilder = new ArrayBuilder<UnsubscribeReasonCode>(12);
         var reasonCodeEnumerator = unsubAck.GetReasonCodes();

         while (reasonCodeEnumerator.MoveNext())
         {
            reasonCodeBuilder.Add(reasonCodeEnumerator.Current);
         }

         if (reasonCodeBuilder.Count != 0 && filterBuilder.Count != reasonCodeBuilder.Count)
         {
            return new StringError("The reason codes length is different to topic filters.");
         }

         string? reasonString = null;
         if (unsubAck.ReasonStringUtf8Bytes.Length > 0)
         {
            reasonString = unsubAck.ReasonStringUtf8Bytes.GetUtf8String();
         }

         var unsubscriptions = new MqttTopicUnsubscriptionResult[filterBuilder.Count];
         for (var i = 0; i < filterBuilder.Count; i++)
         {
            unsubscriptions[i] = new MqttTopicUnsubscriptionResult
            {
               TopicFilter = filterBuilder.WrittenSpan[i],
               ReasonCode = reasonCodeBuilder.Count > i ? reasonCodeBuilder.WrittenSpan[i] : UnsubscribeReasonCode.Success
            };
         }

         return new UnsubscribeResult
         {
            PacketIdentifier = unsubAck.PacketIdentifier,
            ReasonString = reasonString,
            Unsubscriptions = unsubscriptions,
            UserProperties = UserPropertyCollection.Create(unsubAck.PropertiesBytes)
         };
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.UnsubscribeAsync: Error unsubscribing: {0}", error.Message);
         return new StringError(error.ToString());
      }
   }

   public async Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default)
   {
      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult;

      if (_controlStream is not { } stream)
      {
         return new StringError("Invalid control stream.");
      }

      TraceLogger.LogClientInfo("MqttClient.PingAsync: Sending PINGREQ...");

      try
      {
         if (ct.CanBeCanceled)
         {
            await SendAndAck<PingReqPacket, PingRespPacket>(new PingReqPacket(), stream, ct);
         }
         else
         {
            using var tokenSource = new CancellationTokenSource(_connectOptions.Timeout);
            await SendAndAck<PingReqPacket, PingRespPacket>(new PingReqPacket(), stream, tokenSource.Token);
         }
         TraceLogger.LogClientInfo("MqttClient.PingAsync: PINGRESP received successfully.");
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.PingAsync: Ping failed: {0}", error.Message);
         return new StringError(error.ToString());
      }

      return true;
   }

   private Task<TResponse> SendAndAck<TPacket, TResponse>(in TPacket packet, INetworkStream stream, CancellationToken ct = default)
      where TPacket : IRawMqttPacket
   {
      ushort identifier = 0;
      if (packet is not PingReqPacket)
      {
         if (packet is PubRelPacket pubRel)
         {
            identifier = pubRel.PacketIdentifier;
         }
         else
         {
            identifier = _identifierGenerator.GenerateNextIdentifier();
         }
      }

      TraceLogger.LogClientInfo("MqttClient.SendAndAck: Sending packet '{0}' (PacketId: {1}) expecting '{2}'...", typeof(TPacket).Name, identifier, typeof(TResponse).Name);

      var signalAwaiter = _signalBroker.AddAwaitable<TResponse>(identifier);
      try
      {
         var lockTask = stream.AcquireWriterLock(ct);
         if (!lockTask.IsCompletedSuccessfully)
            return SendAndAckSlowAsync(packet, lockTask, signalAwaiter, stream, ct);

         var lockToken = lockTask.Result;
         try
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).Write(in packet);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).Write(in packet);
                  break;
               default:
                  throw new InvalidOperationException("Unknown protocol version.");
            }

            var flushTask = writer.FlushAsync(ct);
            if (!flushTask.IsCompletedSuccessfully)
               return CompleteFlushAndAckAsync(flushTask, lockToken, signalAwaiter, ct);

            // consume flush task
            _ = flushTask.Result.IsCompleted;
            ResetKeepAliveTimestamp();

            lockToken.Dispose();
            return AwaitAck(signalAwaiter, ct);
         }
         catch (Exception error)
         {
            TraceLogger.LogClientError("MqttClient.SendAndAck: Error writing packet '{0}': {1}", typeof(TPacket).Name, error.Message);
            lockToken.Dispose();
            signalAwaiter.Fail(error);

            return AwaitAck(signalAwaiter, ct);
         }
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.SendAndAck: Error acquiring writer lock for '{0}': {1}", typeof(TPacket).Name, error.Message);
         signalAwaiter.Fail(error);
         return AwaitAck(signalAwaiter, ct);
      }
   }

   private Task<TResponse> SendAndAck<TOptions, TResponse>(TOptions options, INetworkStream stream, CancellationToken ct = default)
      where TOptions : class, IHeapMqttOptions
   {
      ushort identifier = 0;
      if (options is not ConnectOptions)
      {
         identifier = _identifierGenerator.GenerateNextIdentifier();
      }

      TraceLogger.LogClientInfo("MqttClient.SendAndAck: Sending options '{0}' (PacketId: {1}) expecting '{2}'...", typeof(TOptions).Name, identifier, typeof(TResponse).Name);
      var signalAwaiter = _signalBroker.AddAwaitable<TResponse>(identifier);

      try
      {
         var lockTask = stream.AcquireWriterLock(ct);
         if (!lockTask.IsCompletedSuccessfully)
            return SendAndAckSlowAsync(options, identifier, lockTask, signalAwaiter, stream, ct);

         var lockToken = lockTask.Result;
         try
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).Write(options, identifier);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).Write(options, identifier);
                  break;
               default:
                  throw new InvalidOperationException("Unknown protocol version.");
            }

            var flushTask = writer.FlushAsync(ct);
            if (!flushTask.IsCompletedSuccessfully)
               return CompleteFlushAndAckAsync(flushTask, lockToken, signalAwaiter, ct);

            // consume flush task
            _ = flushTask.Result.IsCompleted;
            ResetKeepAliveTimestamp();

            lockToken.Dispose();
            return AwaitAck(signalAwaiter, ct);
         }
         catch (Exception error)
         {
            TraceLogger.LogClientError("MqttClient.SendAndAck: Error writing options '{0}': {1}", typeof(TOptions).Name, error.Message);
            lockToken.Dispose();
            signalAwaiter.Fail(error);

            return AwaitAck(signalAwaiter, ct);
         }
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.SendAndAck: Error acquiring writer lock for options '{0}': {1}", typeof(TOptions).Name, error.Message);
         signalAwaiter.Fail(error);

         return AwaitAck(signalAwaiter, ct);
      }
   }

   private static async Task<TResponse> AwaitAck<TResponse>(SignalAwaiter<TResponse> signalAwaiter, CancellationToken ct)
   {
      using (signalAwaiter)
      {
         return await signalAwaiter.WaitOneAsync(ct);
      }
   }

   private async Task<TResponse> CompleteFlushAndAckAsync<TResponse>(
      ValueTask<System.IO.Pipelines.FlushResult> flushTask,
      LockReleaser lockToken,
      SignalAwaiter<TResponse> signalAwaiter,
      CancellationToken ct)
   {
      try
      {
         using (lockToken)
         {
            await flushTask;
            ResetKeepAliveTimestamp();
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
      }

      return await AwaitAck(signalAwaiter, ct);
   }

   private async Task<TResponse> SendAndAckSlowAsync<TPacket, TResponse>(
      TPacket packet,
      ValueTask<LockReleaser> lockTask,
      SignalAwaiter<TResponse> signalAwaiter,
      INetworkStream stream,
      CancellationToken ct)
      where TPacket : IRawMqttPacket
   {
      try
      {
         using (await lockTask)
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).Write(packet);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).Write(packet);
                  break;
               default:
                  throw new InvalidOperationException("Unknown protocol version.");
            }

            await writer.FlushAsync(ct);
            ResetKeepAliveTimestamp();
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
      }

      return await AwaitAck(signalAwaiter, ct);
   }

   private async Task<TResponse> SendAndAckSlowAsync<TOptions, TResponse>(
      TOptions options,
      ushort identifier,
      ValueTask<LockReleaser> lockTask,
      SignalAwaiter<TResponse> signalAwaiter,
      INetworkStream stream,
      CancellationToken ct)
      where TOptions : class, IHeapMqttOptions
   {
      try
      {
         using (await lockTask)
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).Write(options, identifier);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).Write(options, identifier);
                  break;
               default:
                  throw new InvalidOperationException("Unknown protocol version.");
            }

            await writer.FlushAsync(ct);
            ResetKeepAliveTimestamp();
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
      }

      return await AwaitAck(signalAwaiter, ct);
   }

   private Task Send<TPacket>(in TPacket packet, INetworkStream stream, CancellationToken ct = default)
      where TPacket : IRawMqttPacket
   {
      TraceLogger.LogClientInfo("MqttClient.Send: Sending packet '{0}'...", typeof(TPacket).Name);

      if (typeof(TPacket) == typeof(PubAckPacket) ||
          typeof(TPacket) == typeof(PubCompPacket) ||
          packet is PubRecPacket { ReasonCode: >= PubRecReasonCode.UnspecifiedError })
      {
         DecrementIncomingInFlight();
      }

      try
      {
         var lockTask = stream.AcquireWriterLock(ct);
         if (!lockTask.IsCompletedSuccessfully)
            return SendSlowAsync(packet, lockTask, stream, ct);

         var lockToken = lockTask.Result;
         try
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).Write(in packet);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).Write(in packet);
                  break;
               default:
                  throw new InvalidOperationException("Unknown protocol version.");
            }

            var flushTask = writer.FlushAsync(ct);
            if (!flushTask.IsCompletedSuccessfully)
               return CompleteFlushAsync(flushTask, lockToken);

            // consume flush task
            _ = flushTask.Result.IsCompleted;
            ResetKeepAliveTimestamp();

            lockToken.Dispose();
            return Task.CompletedTask;
         }
         catch (Exception error)
         {
            TraceLogger.LogClientError("MqttClient.Send: Error sending packet '{0}': {1}", typeof(TPacket).Name, error.Message);
            lockToken.Dispose();
            return Task.FromException(error);
         }
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.Send: Error acquiring lock for packet '{0}': {1}", typeof(TPacket).Name, error.Message);
         return Task.FromException(error);
      }
   }

   private Task Send<TOptions>(TOptions options, INetworkStream stream, ushort identifier = 0, CancellationToken ct = default)
      where TOptions : class, IHeapMqttOptions
   {
      TraceLogger.LogClientInfo("MqttClient.Send: Sending option packet '{0}' (PacketId: {1})...", typeof(TOptions).Name, identifier);
      try
      {
         var lockTask = stream.AcquireWriterLock(ct);
         if (!lockTask.IsCompletedSuccessfully)
            return SendSlowAsync(options, identifier, lockTask, stream, ct);

         var lockToken = lockTask.Result;
         try
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).Write(options, identifier);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).Write(options, identifier);
                  break;
               default:
                  throw new InvalidOperationException("Unknown protocol version.");
            }

            var flushTask = writer.FlushAsync(ct);
            if (!flushTask.IsCompletedSuccessfully)
               return CompleteFlushAsync(flushTask, lockToken);

            // consume flush task
            _ = flushTask.Result.IsCompleted;
            ResetKeepAliveTimestamp();

            lockToken.Dispose();
            return Task.CompletedTask;
         }
         catch (Exception error)
         {
            TraceLogger.LogClientError("MqttClient.Send: Error sending option packet '{0}': {1}", typeof(TOptions).Name, error.Message);
            lockToken.Dispose();
            return Task.FromException(error);
         }
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.Send: Error acquiring lock for option packet '{0}': {1}", typeof(TOptions).Name, error.Message);
         return Task.FromException(error);
      }
   }

   private async Task CompleteFlushAsync(
      ValueTask<System.IO.Pipelines.FlushResult> flushTask,
      LockReleaser lockToken)
   {
      using (lockToken)
      {
         await flushTask;
         ResetKeepAliveTimestamp();
      }
   }

   private async Task SendSlowAsync<TPacket>(
      TPacket packet,
      ValueTask<LockReleaser> lockTask,
      INetworkStream stream,
      CancellationToken ct)
      where TPacket : IRawMqttPacket
   {
      using (await lockTask)
      {
         var writer = stream.Transport.Output;
         switch (_protocolVersion)
         {
            case MqttProtocolVersion.V50:
               new PacketVersion5Encoder(writer).Write(packet);
               break;
            case MqttProtocolVersion.V31:
            case MqttProtocolVersion.V311:
               new PacketVersion3Encoder(writer, _protocolVersion).Write(packet);
               break;
            default:
               throw new InvalidOperationException("Unknown protocol version.");
         }

         await writer.FlushAsync(ct);
         ResetKeepAliveTimestamp();
      }
   }

   private async Task SendSlowAsync<TOptions>(
      TOptions options,
      ushort identifier,
      ValueTask<LockReleaser> lockTask,
      INetworkStream stream,
      CancellationToken ct)
      where TOptions : class, IHeapMqttOptions
   {
      using (await lockTask)
      {
         var writer = stream.Transport.Output;
         switch (_protocolVersion)
         {
            case MqttProtocolVersion.V50:
               new PacketVersion5Encoder(writer).Write(options, identifier);
               break;
            case MqttProtocolVersion.V31:
            case MqttProtocolVersion.V311:
               new PacketVersion3Encoder(writer, _protocolVersion).Write(options, identifier);
               break;
            default:
               throw new InvalidOperationException("Unknown protocol version.");
         }

         await writer.FlushAsync(ct);
         ResetKeepAliveTimestamp();
      }
   }

   private Task SendConnect(INetworkStream stream, ConnectOptions options, CancellationToken ct = default)
   {
      return Send(options, stream, 0, ct);
   }

     public Task SendAsync<TPacket>(in TPacket packet, CancellationToken ct = default)
      where TPacket : struct, IRawMqttPacket
   {
      var disposedResult = ValidateDisposed();
      if (disposedResult.Failed)
      {
         return Task.FromException(new InvalidOperationException(disposedResult.Error.Detail));
      }

      var state = (MqttClientConnectionState)_state;
      if (state is not MqttClientConnectionState.Connected &&
          !(state is MqttClientConnectionState.Connecting && typeof(TPacket) == typeof(AuthPacket)))
      {
         return Task.FromException(new InvalidOperationException("Client is not connected."));
      }

      if (_controlStream is not { } stream)
      {
         return Task.FromException(new InvalidOperationException("Invalid control stream."));
      }

      TraceLogger.LogClientInfo("MqttClient.SendAsync: Asynchronously sending packet '{0}'...", typeof(TPacket).Name);
      return Send(in packet, stream, ct);
   }
}
