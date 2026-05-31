using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Cluster.Protocol.Interfaces;

namespace Beskar.Networking.Cluster.Protocol.Models;

public sealed class ClusterMessageContext : ISpanFormattable
{
   public required INetworkSession Session { get; init; }

   public required IPacketValidator Validator { get; init; }

   public required IClusterHost Host { get; init; }

   public bool IsJoined { get; init; }

   public string ToString(string? format, IFormatProvider? formatProvider)
   {
      FormattableString formattable =
         $"LN: {Host.LocalNodeId}";

      return formattable.ToString(formatProvider);
   }

   public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format,
      IFormatProvider? provider)
   {
      return destination.TryWrite(provider,
         $"LN: {Host.LocalNodeId}",
         out charsWritten);
   }

   public override string ToString()
   {
      return $"LN: {Host.LocalNodeId}";
   }
}
