using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version3;

[StructLayout(LayoutKind.Auto)]
public readonly ref partial struct PacketVersion3Encoder(
   IBufferWriter<byte> writer,
   MqttProtocolVersion protocolVersion)
{
   private readonly IBufferWriter<byte> _writer = writer;
   private readonly MqttProtocolVersion _protocolVersion = protocolVersion;

   public void Write<TPacket>(in TPacket packet)
      where TPacket : IRawMqttPacket
   {
      if (typeof(TPacket) == typeof(ConnectPacket))
      {
         WriteConnect(in Unsafe.As<TPacket, ConnectPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PublishPacket))
      {
         WritePublish(in Unsafe.As<TPacket, PublishPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubAckPacket))
      {
         WritePubAck(in Unsafe.As<TPacket, PubAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubRecPacket))
      {
         WritePubRec(in Unsafe.As<TPacket, PubRecPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubRelPacket))
      {
         WritePubRel(in Unsafe.As<TPacket, PubRelPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PubCompPacket))
      {
         WritePubComp(in Unsafe.As<TPacket, PubCompPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(SubscribePacket))
      {
         WriteSubscribe(in Unsafe.As<TPacket, SubscribePacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(SubAckPacket))
      {
         WriteSubAck(in Unsafe.As<TPacket, SubAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(UnsubscribePacket))
      {
         WriteUnsubscribe(in Unsafe.As<TPacket, UnsubscribePacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(UnsubAckPacket))
      {
         WriteUnsubAck(in Unsafe.As<TPacket, UnsubAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PingReqPacket))
      {
         WritePingReq(in Unsafe.As<TPacket, PingReqPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(PingRespPacket))
      {
         WritePingResp(in Unsafe.As<TPacket, PingRespPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(ConnAckPacket))
      {
         WriteConnAck(in Unsafe.As<TPacket, ConnAckPacket>(ref Unsafe.AsRef(in packet)));
      }
      else if (typeof(TPacket) == typeof(DisconnectPacket))
      {
         WriteDisconnect(in Unsafe.As<TPacket, DisconnectPacket>(ref Unsafe.AsRef(in packet)));
      }
      else
      {
         throw new NotSupportedException($"Packet type {typeof(TPacket).Name} is not supported by PacketVersion3Encoder.");
      }
   }

   public void Write<TOptions>(TOptions options, ushort packetIdentifier = 0)
      where TOptions : class, IHeapMqttOptions
   {
      if (typeof(TOptions) == typeof(ConnectOptions))
      {
         WriteConnect(Unsafe.As<TOptions, ConnectOptions>(ref options));
      }
      else if (typeof(TOptions) == typeof(PublishOptions))
      {
         WritePublish(Unsafe.As<TOptions, PublishOptions>(ref options), packetIdentifier);
      }
      else if (typeof(TOptions) == typeof(SubscribeOptions))
      {
         WriteSubscribe(Unsafe.As<TOptions, SubscribeOptions>(ref options), packetIdentifier);
      }
      else if (typeof(TOptions) == typeof(UnsubscribeOptions))
      {
         WriteUnsubscribe(Unsafe.As<TOptions, UnsubscribeOptions>(ref options), packetIdentifier);
      }
      else if (typeof(TOptions) == typeof(DisconnectOptions))
      {
         WriteDisconnect(Unsafe.As<TOptions, DisconnectOptions>(ref options));
      }
      else
      {
         throw new NotSupportedException($"Options type {typeof(TOptions).Name} is not supported by PacketVersion3Encoder.");
      }
   }
}
