namespace Beskar.Mqtt.Server.Enumerators;


/// <summary>
/// A zero-allocation (during routing/matching) ref struct enumerator to split MQTT topic levels.
/// </summary>
public ref struct TopicLevelEnumerator(ReadOnlySpan<byte> span)
{
   private readonly ReadOnlySpan<byte> _span = span;
   private int _position = 0;
   private int _nextSeparator = -2; // special initial state

   public ReadOnlySpan<byte> Current { get; private set; }

   public bool MoveNext()
   {
      switch (_nextSeparator)
      {
         case -1:
            return false;
         case -2:
         {
            // First level
            _nextSeparator = _span.IndexOf((byte)0x2F); // '/'

            if (_nextSeparator == -1)
            {
               Current = _span;
               return true;
            }

            Current = _span[.._nextSeparator];
            _position = _nextSeparator + 1;

            return true;
         }
      }

      var remaining = _span[_position..];
      _nextSeparator = remaining.IndexOf((byte)0x2F);

      if (_nextSeparator == -1)
      {
         Current = remaining;
         return true;
      }

      Current = remaining[.._nextSeparator];
      _position += _nextSeparator + 1;

      return true;
   }
}

