using System.Buffers;
using Beskar.Mqtt.Client.Responses;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client;

public sealed class MqttClient
{
   private readonly INetworkClient _networkClient;

   internal MqttClient(INetworkClient client)
   {
      _networkClient = client;
   }

   public ValueTask<ClientConnectResponse> ConnectAsync(CancellationToken ct = default)
   {
      return new ValueTask<ClientConnectResponse>(new ClientConnectResponse());
   }

   /// <summary>
   /// Internal message receive and parse loop
   /// </summary>
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

            if (buffer.IsEmpty && result.IsCompleted)
            {
               break;
            }

            if (!buffer.IsEmpty)
            {
               var sequenceReader = new SequenceReader<byte>(buffer);
               // Call parser

               var parsed = 0;
               var consumed = buffer.GetPosition(parsed);
               reader.AdvanceTo(consumed, buffer.End);
            }
            else
            {
               reader.AdvanceTo(buffer.End);
            }
         }
      }
      catch (OperationCanceledException)
      {
         // expected
      }
   }
}
