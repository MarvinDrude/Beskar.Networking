using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Extensions;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Parsers.Version3;
using Beskar.Mqtt.Common.Parsers.Version5;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Parsers;

[StructLayout(LayoutKind.Auto)]
public ref struct PacketParser(
   IPacketHandler handler,
   MqttProtocolVersion protocolVersion)
{
   private readonly IPacketHandler _packetHandler = handler;
   private MqttProtocolVersion _protocolVersion = protocolVersion;
   private bool _tryPrivate;

   public ValueTask<Result<PacketDispatchResult, StringError>> TryDispatch(
      ref SequenceReader<byte> reader,
      out int bytesConsumed,
      CancellationToken cancellation = default)
   {
      bytesConsumed = 0;

      if (reader.Remaining < 2)
      {
         // not enough data for a full packet
         return ValueTask.FromResult<Result<PacketDispatchResult, StringError>>(PacketDispatchResult.NotEnoughData);
      }

      if (TryParseBodyLength(ref reader, out var fixedHeader,
             out var headerLength, out var bodyLength)
          is not PacketDispatchResult.Success and var res)
      {
         // not enough data or protocol error
         return ValueTask.FromResult<Result<PacketDispatchResult, StringError>>(res);
      }

      if (reader.Remaining < bodyLength)
      {
         // enough data for the entire body not provided
         return ValueTask.FromResult<Result<PacketDispatchResult, StringError>>(PacketDispatchResult.NotEnoughData);
      }

      var rawPacket = new RawPacket(fixedHeader, headerLength + bodyLength, bodyLength)
      {
         Reader = reader
      };

      if (_protocolVersion is MqttProtocolVersion.Unknown)
      {
         var protocolResult = ParseProtocolVersion(ref rawPacket, out _tryPrivate);
         if (protocolResult.Failed)
            return ValueTask.FromResult<Result<PacketDispatchResult, StringError>>(protocolResult.Error);

         _protocolVersion = protocolResult.Success;
      }

      var innerConsumed = 0;
      var dispatchTask = _protocolVersion switch
      {
         MqttProtocolVersion.V50 => new PacketVersion5Parser(_packetHandler)
            .TryDispatch(ref rawPacket, out innerConsumed, cancellation),
         MqttProtocolVersion.V311 or MqttProtocolVersion.V31 => new PacketVersion3Parser(_packetHandler, _protocolVersion)
            .TryDispatch(ref rawPacket, out innerConsumed, cancellation),
         _ => ValueTask.FromResult(PacketDispatchResult.ProtocolError)
      };

      // innserConsumed is always set in sync path
      bytesConsumed += innerConsumed;

      return dispatchTask.IsCompletedSuccessfully
         ? ValueTask.FromResult<Result<PacketDispatchResult, StringError>>(dispatchTask.Result)
         : Awaited(dispatchTask);

      static async ValueTask<Result<PacketDispatchResult, StringError>> Awaited(
         ValueTask<PacketDispatchResult> task)
      {
         var result = await task;
         return result;
      }
   }

   /// <summary>
   /// Tries to read the variable encoded length information from
   /// the packet.
   /// </summary>
   private static PacketDispatchResult TryParseBodyLength(
      ref SequenceReader<byte> reader,
      out byte fixedHeader,
      out int headerLength,
      out int bodyLength)
   {
      headerLength = 0;
      bodyLength = 0;
      fixedHeader = 0;

      var copyReader = reader;
      int value = 0, shift = 0, bytesRead = 0;
      byte bcode;

      if (!copyReader.TryRead(out fixedHeader))
      {
         return PacketDispatchResult.NotEnoughData;
      }

      do
      {
         if (!copyReader.TryRead(out bcode))
         {
            return PacketDispatchResult.NotEnoughData;
         }
         bytesRead++;

         value |= (bcode & 0x7F) << shift;
         shift += 7;

         if (shift > 21 && (bcode & 0x80) != 0)
         {
            return PacketDispatchResult.ProtocolError;
         }
      }
      while ((bcode & 0x80) != 0);

      // apply the read changes
      reader = copyReader;

      headerLength = 1 + bytesRead;
      bodyLength = value;

      return PacketDispatchResult.Success;
   }

   /// <summary>
   /// Tries parsing the requested protocol version
   /// </summary>
   private static Result<MqttProtocolVersion, StringError> ParseProtocolVersion(ref RawPacket packet, out bool tryPrivate)
   {
      tryPrivate = false;

      if (packet.Reader.Remaining < 7)
      {
         return new StringError("First packet (CONNECT) must have atleast 7 bytes.");
      }

      if (!packet.Reader.TryReadUInt16BigEndian(out var stringLength))
      {
         return new StringError("Unable to read (CONNECT) protocol length.");
      }

      if (stringLength > 256)
      {
         return new StringError("Version string is out of bounds.");
      }

      if (packet.Reader.Remaining < stringLength + 1)
      {
         return new StringError("Not enough data found on (CONNECT) packet.");
      }

      Span<byte> utf8Bytes = stackalloc byte[stringLength];
      if (!packet.Reader.TryCopyTo(utf8Bytes))
      {
         return new StringError("Unable to copy utf8 bytes to buffer.");
      }
      packet.Reader.Advance(stringLength);

      if (!packet.Reader.TryRead(out var protocolCode))
      {
         return new StringError("Could not read protocol code.");
      }

      // Remove the mosquitto try_private flag (MQTT 3.1.1 Bridge)
      tryPrivate = (protocolCode & 0x80) > 0;
      protocolCode &= 0x7F;

      if (utf8Bytes.SequenceEqual("MQTT"u8))
      {
         if (protocolCode == 5)
         {
            return MqttProtocolVersion.V50;
         }

         if (protocolCode == 4)
         {
            return MqttProtocolVersion.V311;
         }

         return new StringError($"Protocol level '{protocolCode}' not supported for protocol 'MQTT'.");
      }

      if (utf8Bytes.SequenceEqual("MQIsdp"u8))
      {
         if (protocolCode == 3)
         {
            return MqttProtocolVersion.V31;
         }

         return new StringError($"Protocol level '{protocolCode}' not supported for protocol 'MQIsdp'.");
      }

      return new StringError("The specified protocol name is not supported.");
   }
}
