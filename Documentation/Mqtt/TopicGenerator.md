# MQTT Topic Source Generator

Beskar.Mqtt includes an automatic, high-performance, compile-time C# Source Generator for formatting and parsing MQTT topics.
By declaring your topic routing patterns as compile-time templates, the generator automatically generates strongly-typed parser
and formatter methods that run with **zero allocations** and **compile-time validation**.

---

## 1. Defining Topics

To define a compile-time topic, mark a `static partial` method inside a `partial class` or `partial struct`
with the `[GeneratedMqttTopic]` attribute:

```csharp
using System;
using Beskar.Mqtt.Common.Generators;

public static partial class Topics
{
   // Parser: matches pattern "devices/{deviceId}/status/{isOk}"
   [GeneratedMqttTopic("devices/{deviceId}/status/{isOk}")]
   public static partial bool TryParseStatus(
      ReadOnlySpan<char> topic,
      out int deviceId,
      out bool isOk);

   // Formatter: formats parameters into a destination span
   [GeneratedMqttTopic("devices/{deviceId}/status/{isOk}")]
   public static partial bool TryFormatStatus(
      Span<char> destination,
      int deviceId,
      bool isOk,
      out int charsWritten);
}
```

### Supported Types
The generator natively parses and formats:
* Primitives (`int`, `long`, `double`, `float`, `bool`, `byte`, etc.)
* String types (`string`, `ReadOnlySpan<char>`, `ReadOnlySpan<byte>`)
* Specialized BCL types (`Guid`)
* Custom user `enum` types (which are parsed by name/integer and formatted allocation-free via `Enum.TryFormat`).

---

## 2. Generated Code API

For every `TryFormat` partial method declaration, the generator automatically outputs two convenience methods
as sibling overloads inside the same partial class:

### A. String-Returning Helper
```csharp
public static string FormatStatus(int deviceId, bool isOk);
```

* **Under the Hood**: Uses `TextWriterIndentSlim` with a stack-allocated buffer of 256 characters.
* **Benefits**: Allocation-free formatting up to the stack buffer size, returning only the final constructed string.

### B. Byte Array-Returning Helper (Preferred)
```csharp
public static byte[] FormatStatusToBytes(int deviceId, bool isOk);
```
* **Under the Hood**: Uses `BufferWriter<byte>` with a stack-allocated buffer of 256 bytes (automatically resizing if exceeded).
Primitives are written natively using `Utf8Formatter.TryFormat`, strings are converted to UTF-8 in-place, and literals use `u8` string constants.
* **Benefits**: Zero temporary allocations, returning only the final `byte[]`.

---

## 3. Performance Tip: Zero-Allocation Topic Publishing

Beskar.Mqtt's `PublishOptions` builder supports passing topics directly as **UTF-8 Spans** (`ReadOnlySpan<byte>`).

> [!TIP]
> **Always prefer using the `ToBytes` formatting helper.**
> By formatting the topic name directly to a byte array and passing it to `.WithTopic(...)`, you completely bypass C#
string allocation overhead, achieving end-to-end zero-allocation publishing.

```csharp
// 1. Format the topic directly to UTF-8 bytes
byte[] topicBytes = Topics.FormatStatusToBytes(42, true);

// 2. Publish using the byte array directly (supports ReadOnlySpan<byte>)
var publishOptions = PublishOptions.Create()
   .WithTopic(topicBytes) // Zero-allocation topic assignment!
   .WithPayload("Normal Operations")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();

await publisherClient.PublishAsync(publishOptions);
```

---

## 4. Wildcard Subscriptions and Parsing Example

Below is a typical real-time telemetry processing loop demonstrating wildcard subscriptions and generated topic matching:

> [!NOTE]
> Usually it is preferable to use the payload for data and not put everything in the topic like here.

```csharp
// Subscribe to wildcard topic
var subscribeOptions = SubscribeOptions.Create()
   .WithTopicFilter("devices/+/status/+", QualityOfServiceType.AtLeastOnce)
   .Build();
await subscriberClient.SubscribeAsync(subscribeOptions);

// Receive and match topics in callback
subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var topicSpan = context.Message.Topic.AsSpan();
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);

   // Parse topic fields using the generated TryParse method
   if (Topics.TryParseStatus(topicSpan, out var deviceId, out var isOk))
   {
      Console.WriteLine($"Device {deviceId} status updated: IsOk = {isOk}");
   }

   return ValueTask.CompletedTask;
});
```
