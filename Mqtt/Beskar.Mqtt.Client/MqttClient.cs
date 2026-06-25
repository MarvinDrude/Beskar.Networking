using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client.Responses;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Results;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client;

public sealed class MqttClient : IMqttClient
{
   private readonly INetworkClient _networkClient;
   private readonly IPacketHandler _packetHandler = null!;

   internal MqttClient(INetworkClient client)
   {
      _networkClient = client;
   }

   public ValueTask<ClientConnectResponse> ConnectAsync(CancellationToken ct = default)
   {
      return new ValueTask<ClientConnectResponse>(new ClientConnectResponse());
   }

   public Task<Result<PublishResult, StringError>> PublishAsync(
      PublishOptions options, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public Task<Result<SubscribeResult, StringError>> SubscribeAsync(
      SubscribeOptions options, CancellationToken ct = default)
   {

   }

   public Task<Result<UnsubscribeResult, StringError>> UnsubscribeAsync(
      UnsubscribeOptions options, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   private async Task RunMessageReceive(INetworkStream networkStream, CancellationToken ct = default)
   {
      try
      {
         // duplex input for reading incoming messages
         var reader = networkStream.Transport.Input;

         while (true)
         {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (result.IsCanceled) break;
            if (buffer.IsEmpty && result.IsCompleted) break;

            var consumed = buffer.Start;
            var examined = buffer.End;

            while (!buffer.IsEmpty)
            {
               var sequenceReader = new SequenceReader<byte>(buffer);
               var parser = new PacketParser(_packetHandler, MqttProtocolVersion.V50);
               var valueTask = parser.TryDispatch(ref sequenceReader, out var parsedBytes, ct);

               var parseResult = valueTask.IsCompletedSuccessfully
                  ? valueTask.Result
                  : await valueTask.ConfigureAwait(false);

               if (parseResult.Failed || parseResult.Success is PacketDispatchResult.ProtocolError
                      or PacketDispatchResult.InvalidPacketType)
               {
                  // Protocol violation: exit the loop to drop the connection
                  return;
               }

               if (parseResult.Success is PacketDispatchResult.NotEnoughData)
               {
                  break;
               }

               consumed = buffer.GetPosition(parsedBytes);
               buffer = buffer.Slice(consumed);
            }

            reader.AdvanceTo(consumed, examined);
            if (result.IsCompleted && buffer.IsEmpty) break;
         }
      }
      catch (OperationCanceledException)
      {
         // expected
      }
   }

   public ValueTask DisposeAsync()
   {
      throw new NotImplementedException();
   }
}
