using System.Net;

namespace Beskar.Networking.Transports.Memory;

/// <summary>
/// Represents an in-memory transport endpoint.
/// </summary>
public sealed class MemoryEndPoint : EndPoint
{
   public string Address { get; }

   public MemoryEndPoint(string address)
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(address);
      Address = address;
   }

   public override string ToString() => $"in-memory://{Address}";

   public override bool Equals(object? obj)
      => obj is MemoryEndPoint other && Address.Equals(other.Address, StringComparison.OrdinalIgnoreCase);

   public override int GetHashCode()
      => Address.GetHashCode(StringComparison.OrdinalIgnoreCase);
}
