namespace Beskar.Mqtt.Common.Generators;

public sealed class PacketIdentifierGenerator
{
   private int _value;

   public ushort GenerateNextIdentifier()
   {
      int current;
      int next;

      do
      {
         current = _value;
         next = current >= 65535 ? 1 : current + 1;
      }
      while (Interlocked.CompareExchange(ref _value, next, current) != current);

      return (ushort)next;
   }

   public void Reset()
   {
      Interlocked.Exchange(ref _value, 0);
   }
}
