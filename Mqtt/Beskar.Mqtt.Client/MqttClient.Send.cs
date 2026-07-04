using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   public Task<Result<PublishResult, StringError>> PublishAsync(
      PublishOptions options, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

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



      if (ct.CanBeCanceled)
      {
         await SendAndAck<Subscribe, SubAckPacket>(new PingReqPacket(), stream, ct);
      }
      else
      {
         using var tokenSource = new CancellationTokenSource(_connectOptions.Timeout);
         await SendAndAck<PingReqPacket, SubAckPacket>(new PingReqPacket(), stream, tokenSource.Token);
      }

      return true;
   }

   public Task<Result<UnsubscribeResult, StringError>> UnsubscribeAsync(
      UnsubscribeOptions options, CancellationToken ct = default)
   {

      throw new NotImplementedException();
   }

   public async Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default)
   {
      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult;

      if (_controlStream is not { } stream)
      {
         return new StringError("Invalid control stream.");
      }

      if (ct.CanBeCanceled)
      {
         await SendAndAck<PingReqPacket, PingRespPacket>(new PingReqPacket(), stream, ct);
      }
      else
      {
         using var tokenSource = new CancellationTokenSource(_connectOptions.Timeout);
         await SendAndAck<PingReqPacket, PingRespPacket>(new PingReqPacket(), stream, tokenSource.Token);
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
