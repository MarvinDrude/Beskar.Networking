using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
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
      }
      catch (Exception error)
      {
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
         identifier = _identifierGenerator.GenerateNextIdentifier();
      }

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

            flushTask.GetAwaiter().GetResult();
            lockToken.Dispose();

            return signalAwaiter.WaitOneAsync(ct).AsTask();
         }
         catch (Exception error)
         {
            lockToken.Dispose();
            signalAwaiter.Fail(error);

            return signalAwaiter.WaitOneAsync(ct).AsTask();
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
         return signalAwaiter.WaitOneAsync(ct).AsTask();
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

            flushTask.GetAwaiter().GetResult();
            lockToken.Dispose();

            return signalAwaiter.WaitOneAsync(ct).AsTask();
         }
         catch (Exception error)
         {
            lockToken.Dispose();
            signalAwaiter.Fail(error);

            return signalAwaiter.WaitOneAsync(ct).AsTask();
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
         return signalAwaiter.WaitOneAsync(ct).AsTask();
      }
   }

   private static async Task<TResponse> CompleteFlushAndAckAsync<TResponse>(
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
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
      }

      return await signalAwaiter.WaitOneAsync(ct);
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
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
      }

      return await signalAwaiter.WaitOneAsync(ct);
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
         }
      }
      catch (Exception error)
      {
         signalAwaiter.Fail(error);
      }

      return await signalAwaiter.WaitOneAsync(ct);
   }

   private async Task SendConnect(INetworkStream stream, ConnectOptions options, CancellationToken ct = default)
   {
      using (await stream.AcquireWriterLock(ct))
      {
         var writer = stream.Transport.Output;
         switch (_protocolVersion)
         {
            case MqttProtocolVersion.V50:
               new PacketVersion5Encoder(writer).WriteConnect(options);
               break;
            case MqttProtocolVersion.V31:
            case MqttProtocolVersion.V311:
               new PacketVersion3Encoder(writer, _protocolVersion).WriteConnect(options);
               break;
            default:
               throw new InvalidOperationException("Unkown protocol version.");
         }

         await writer.FlushAsync(ct);
      }
   }
}
