using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Text;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Protocol.Tests;

public struct CustomMagicPacket : IFramingProtocol<CustomMagicPacket>
{
   public const ushort Magic = 0xBEEF;

   public ResilientFrameKind Kind { get; set; }
   public ReadOnlySequence<byte> Payload { get; set; }

   public ResilientFrameKind GetFrameKind()
   {
      return Kind;
   }

   public ReadOnlySequence<byte> GetPayloadSequence()
   {
      return Payload;
   }

   public int GetEncodedLength()
   {
      return 5 + (int)Payload.Length;
   }

   public bool TryWrite(Span<byte> destination, out int bytesWritten)
   {
      var totalLen = GetEncodedLength();
      if (destination.Length < totalLen)
      {
         bytesWritten = 0;
         return false;
      }

      BinaryPrimitives.WriteUInt16BigEndian(destination[..2], Magic);
      destination[2] = (byte)Kind;
      BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(3, 2), (ushort)Payload.Length);

      if (!Payload.IsEmpty) Payload.CopyTo(destination[5..]);

      bytesWritten = totalLen;
      return true;
   }

   public bool TryGetPayload<TPayload>(out TPayload? payload) where TPayload : class, IResilientPayload
   {
      payload = null;
      if (Payload.IsEmpty) return false;

      if (typeof(TPayload) == typeof(ConnectPacketPayload))
      {
         var seq = GetPayloadSequence();
         var reader = new SequenceReader<byte>(seq);
         if (ConnectPacketPayload.TryRead(ref reader, out var connectPayload))
         {
            payload = connectPayload as TPayload;
            return payload != null;
         }
      }
      else if (typeof(TPayload) == typeof(DisconnectPacketPayload))
      {
         var seq = GetPayloadSequence();
         var reader = new SequenceReader<byte>(seq);
         if (DisconnectPacketPayload.TryRead(ref reader, out var disconnectPayload))
         {
            payload = disconnectPayload as TPayload;
            return payload != null;
         }
      }
      else if (typeof(TPayload) == typeof(AuthenticatePacketPayload))
      {
         var seq = GetPayloadSequence();
         var reader = new SequenceReader<byte>(seq);
         if (AuthenticatePacketPayload.TryRead(ref reader, out var authPayload))
         {
            payload = authPayload as TPayload;
            return payload != null;
         }
      }

      return false;
   }

   public static bool TryRead(ref SequenceReader<byte> reader, out CustomMagicPacket frame)
   {
      frame = default;
      if (reader.Remaining < 5) return false;

      var initialConsumed = reader.Consumed;

      if (!reader.TryReadBigEndian(out short magic) || (ushort)magic != Magic)
      {
         reader.Rewind(reader.Consumed - initialConsumed);
         return false;
      }

      if (!reader.TryRead(out var kindByte))
      {
         reader.Rewind(reader.Consumed - initialConsumed);
         return false;
      }

      if (!reader.TryReadBigEndian(out short lenShort))
      {
         reader.Rewind(reader.Consumed - initialConsumed);
         return false;
      }

      int payloadLen = (ushort)lenShort;
      if (reader.UnreadSequence.Length < payloadLen)
      {
         reader.Rewind(reader.Consumed - initialConsumed);
         return false;
      }

      var payloadSeq = payloadLen > 0
         ? new ReadOnlySequence<byte>(reader.UnreadSequence.Slice(0, payloadLen).ToArray())
         : ReadOnlySequence<byte>.Empty;

      reader.Advance(payloadLen);

      frame = new CustomMagicPacket
      {
         Kind = (ResilientFrameKind)kindByte,
         Payload = payloadSeq
      };
      return true;
   }

   public void WriteTo(IBufferWriter<byte> writer)
   {
      var totalLen = GetEncodedLength();
      var span = writer.GetSpan(totalLen);
      if (TryWrite(span, out var written)) writer.Advance(written);
   }

   public static CustomMagicPacket CreateFrame(ResilientFrameKind kind)
   {
      return new CustomMagicPacket
      {
         Kind = kind,
         Payload = ReadOnlySequence<byte>.Empty
      };
   }

   public static CustomMagicPacket CreateFrame(ResilientFrameKind kind, ReadOnlySequence<byte> payload)
   {
      return new CustomMagicPacket
      {
         Kind = kind,
         Payload = payload
      };
   }
}

public class ResilientCustomFramingTests
{
   [Test]
   public async Task Client_And_Server_WithCustomFramingProtocol_ShouldHandshake_And_ExchangeMessages()
   {
      var endpoint = new MemoryEndPoint($"custom_framing_{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var serverOptions = new ResilientServerOptions();
      var server = new ResilientServer<CustomMagicPacket>([listener], serverOptions);

      var serverReceivedTcs = new TaskCompletionSource<string>();

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         serverReceivedTcs.TrySetResult(text);
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<CustomMagicPacket>(clientOptions: new ResilientClientOptions
      {
         Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
      });

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
      var connectResult = await client.ConnectAsync(endpoint, cts.Token);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      var customPayload = "Hello Custom Magic Packet Protocol!"u8.ToArray();
      var frame = CustomMagicPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(customPayload));
      await client.SendAsync(frame, cts.Token);

      var receivedText = await serverReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
      await Assert.That(receivedText).IsEqualTo("Hello Custom Magic Packet Protocol!");

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
