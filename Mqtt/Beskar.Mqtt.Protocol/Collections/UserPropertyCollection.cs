using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Protocol.Collections;

public sealed class UserPropertyCollection : IReadOnlyList<MqttUserProperty>
{
   public int Count
   {
      get
      {
         Materialize();
         return _materialized.Count;
      }
   }

   public MqttUserProperty this[int index]
   {
      get
      {
         Materialize();
         return _materialized[index];
      }
   }

   private readonly ReadOnlySequence<byte>? _underlyingData;
   private List<MqttUserProperty>? _materialized;

   internal UserPropertyCollection(ReadOnlySequence<byte> data)
   {
      _underlyingData = new ReadOnlySequence<byte>(data.ToArray());
   }

   internal UserPropertyCollection(ReadOnlyMemory<byte> data)
   {
      _underlyingData = new ReadOnlySequence<byte>(data);
   }

   public IEnumerator<MqttUserProperty> GetEnumerator()
   {
      Materialize();
      return _materialized.GetEnumerator();
   }

   IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

   public MqttPropertyEnumerator GetDirectEnumerator()
   {
      if (_underlyingData is null) return new MqttPropertyEnumerator(ReadOnlySequence<byte>.Empty);
      return new  MqttPropertyEnumerator(_underlyingData.Value);
   }

   [MemberNotNull(nameof(_materialized))]
   private void Materialize()
   {
      if (_materialized is not null) return;
      _materialized = [];

      if (_underlyingData is null) return;
      var enumerator = GetDirectEnumerator();

      while (enumerator.MoveNext())
      {
         var current = enumerator.Current;
         if (current.Identifier is not PropertyIdentifier.UserProperty)
            continue;

         _materialized.Add(new MqttUserProperty(current));
      }
   }

   public static UserPropertyCollection Create(ReadOnlySpan<byte> data) => [with(data.ToArray())];
   public static UserPropertyCollection Create(ReadOnlyMemory<byte> data) => [with(data)];
   public static UserPropertyCollection Create(ReadOnlySequence<byte> data) => [with(data)];
}
