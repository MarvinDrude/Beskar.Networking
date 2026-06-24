using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Models;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public readonly ref struct TopicFilter(
   ReadOnlySequence<byte> topicUtf8Bytes,
   QualityOfServiceType qualityOfService,
   bool noLocal = false,
   bool retainAsPublished = false,
   RetainHandlingType retainHandling = RetainHandlingType.SendAtSubscription)
{
   public readonly ReadOnlySequence<byte> TopicUtf8Bytes = topicUtf8Bytes;
   public readonly QualityOfServiceType QualityOfService = qualityOfService;

   public readonly bool NoLocal = noLocal;
   public readonly bool RetainAsPublished = retainAsPublished;

   public readonly RetainHandlingType RetainHandling = retainHandling;

   public override string ToString()
   {
      return $"TopicFilter (QoS={QualityOfService}, NoLocal={NoLocal}, RetainAsPublished={RetainAsPublished}, RetainHandling={RetainHandling})";
   }

   internal string DebuggerDisplay => ToString();
}
