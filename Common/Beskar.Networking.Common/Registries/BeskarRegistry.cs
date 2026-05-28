using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Beskar.Memory.Code.PacketGenerator.Common;
using Beskar.Memory.Serialization;
using Beskar.Memory.Writers;

namespace Beskar.Networking.Common.Registries;

public abstract class BeskarRegistry<TState>(BeSerializerOptions? options = null)
   : BasePacketRegistry<TState>
{
   private readonly BeSerializerOptions _options = options ?? BeskarRegistryDefaults._defaultOptions;

   public override bool TryDeserialize<T>(
      ref SequenceReader<byte> reader,
      [MaybeNullWhen(false)] out T packet)
   {
      return BeSerializer.TryDeserialize(ref reader, out packet, _options);
   }

   public override void Serialize<T>(
      ref BufferWriter<byte> writer, T packet)
   {
      BeSerializer.Serialize(packet, ref writer, _options);
   }
}

file static class BeskarRegistryDefaults
{
   public static readonly BeSerializerOptions _defaultOptions = new()
   {
      MaxCollectionLength = 1024,
      MaxDepth = 16
   };
}
