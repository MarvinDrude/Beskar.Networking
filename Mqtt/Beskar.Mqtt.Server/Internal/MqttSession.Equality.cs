using System;
using System.Linq;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession : IEquatable<MqttSession>
{
   public bool Equals(MqttSession? other)
   {
      if (other is null)
         return false;
      if (ReferenceEquals(this, other))
         return true;

      return ClientIdUtf8Bytes.SequenceEqual(other.ClientIdUtf8Bytes);
   }

   public override bool Equals(object? obj)
   {
      return Equals(obj as MqttSession);
   }

   public override int GetHashCode()
   {
      var hashCode = new HashCode();
      hashCode.AddBytes(ClientIdUtf8Bytes);

      return hashCode.ToHashCode();
   }

   public static bool operator ==(MqttSession? left, MqttSession? right)
   {
      if (left is null) return right is null;
      return left.Equals(right);
   }

   public static bool operator !=(MqttSession? left, MqttSession? right)
   {
      return !(left == right);
   }
}
