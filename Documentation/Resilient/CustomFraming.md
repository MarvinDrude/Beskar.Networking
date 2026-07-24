# Resilient Custom Framing Protocol

To optimize payload size, implement custom application protocols, or guarantee interoperability with existing message schemas, you can define a **Custom Framing Protocol** rather than using the default `BeskarPacket` framing.

The Resilient Client and Server wrappers are fully generic over the framing structure:
* `ResilientServer<TFrame>`
* `ResilientClient<TFrame>`

Any struct used as `TFrame` must implement the `IFramingProtocol<TFrame>` contract. This can be achieved in two ways:
1. **Manually implementing `IFramingProtocol<T>`** (giving you absolute, low-level byte control).
2. **Using the Source Generator `[GenerateFramingProtocol]`** (which automatically writes high-performance parsing boilerplate based on field attributes).

---

## 1. The `IFramingProtocol<TSelf>` Contract

To implement custom framing manually, define a struct conforming to the following interface:

```csharp
namespace Beskar.Networking.Protocol;

public interface IFramingProtocol<TSelf> where TSelf : struct, IFramingProtocol<TSelf>
{
    // The total encoded byte length of this specific frame instance
    int GetEncodedLength();

    // Attempts to serialize this frame into a destination span
    bool TryWrite(Span<byte> destination, out int bytesWritten);

    // Writes this frame to an IBufferWriter (often delegates to TryWrite)
    void WriteTo(IBufferWriter<byte> writer);

    // Attempts to read/parse a frame from a SequenceReader.
    // Must return false and rewind/preserve reader position if bytes are incomplete.
    static abstract bool TryRead(ref SequenceReader<byte> reader, out TSelf result);

    // Gets the kind of the frame (Message, Ping, Pong, Connect, etc.)
    ResilientFrameKind GetFrameKind();

    // Factory methods to build frame instances
    static abstract TSelf CreateFrame(ResilientFrameKind kind);
    static abstract TSelf CreateFrame(ResilientFrameKind kind, ReadOnlySequence<byte> payload);

    // Gets the raw payload byte sequence inside the frame
    ReadOnlySequence<byte> GetPayloadSequence();

    // Parses a control packet payload (Connect, Authenticate, Disconnect) out of the frame
    bool TryGetPayload<TPayload>(out TPayload? payload) where TPayload : class, IResilientPayload;
}
```

---

## 2. Option A: Manual Implementation Example

Here is a complete custom protocol frame example named `CustomMagicPacket` which uses a `0xBEEF` magic header, a 1-byte kind field, a 2-byte payload length field, and a trailing payload body.

```csharp
using System.Buffers;
using System.Buffers.Binary;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;

public struct CustomMagicPacket : IFramingProtocol<CustomMagicPacket>
{
    public const ushort MagicHeader = 0xBEEF;

    public ResilientFrameKind Kind { get; set; }
    public ReadOnlySequence<byte> Payload { get; set; }

    public ResilientFrameKind GetFrameKind() => Kind;
    public ReadOnlySequence<byte> GetPayloadSequence() => Payload;
    public int GetEncodedLength() => 5 + (int)Payload.Length;

    public bool TryWrite(Span<byte> destination, out int bytesWritten)
    {
        var totalLen = GetEncodedLength();
        if (destination.Length < totalLen)
        {
            bytesWritten = 0;
            return false;
        }

        // Write Magic Header (2 Bytes)
        BinaryPrimitives.WriteUInt16BigEndian(destination[..2], MagicHeader);
        // Write Frame Kind (1 Byte)
        destination[2] = (byte)Kind;
        // Write Payload Length (2 Bytes)
        BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(3, 2), (ushort)Payload.Length);
        
        // Copy Payload bytes
        if (!Payload.IsEmpty)
        {
            Payload.CopyTo(destination[5..]);
        }

        bytesWritten = totalLen;
        return true;
    }

    public static bool TryRead(ref SequenceReader<byte> reader, out CustomMagicPacket frame)
    {
        frame = default;
        if (reader.Remaining < 5) return false;

        var startPosition = reader.Consumed;

        // Read and check Magic Header
        if (!reader.TryReadBigEndian(out short magic) || (ushort)magic != MagicHeader)
        {
            reader.Rewind(reader.Consumed - startPosition);
            return false;
        }

        // Read Frame Kind
        if (!reader.TryRead(out var kindByte))
        {
            reader.Rewind(reader.Consumed - startPosition);
            return false;
        }

        // Read Payload Length
        if (!reader.TryReadBigEndian(out short lenShort))
        {
            reader.Rewind(reader.Consumed - startPosition);
            return false;
        }

        int payloadLen = (ushort)lenShort;
        if (reader.UnreadSequence.Length < payloadLen)
        {
            // Frame is incomplete, wait for more data to arrive
            reader.Rewind(reader.Consumed - startPosition);
            return false;
        }

        // Parse Payload
        var payloadSeq = payloadLen > 0
            ? new ReadOnlySequence<byte>(reader.UnreadSequence.Slice(0, payloadLen).ToArray())
            : ReadOnlySequence<byte>.Empty;

        reader.Advance(payloadLen);

        frame = new CustomMagicPacket
        {
            Kind = (ResilientFrameKind)kindByte,
            Payload = payloadSeq
        };
        return true;
    }

    public void WriteTo(IBufferWriter<byte> writer)
    {
        var totalLen = GetEncodedLength();
        var span = writer.GetSpan(totalLen);
        if (TryWrite(span, out var written))
        {
            writer.Advance(written);
        }
    }

    public static CustomMagicPacket CreateFrame(ResilientFrameKind kind) => 
        new() { Kind = kind, Payload = ReadOnlySequence<byte>.Empty };

    public static CustomMagicPacket CreateFrame(ResilientFrameKind kind, ReadOnlySequence<byte> payload) => 
        new() { Kind = kind, Payload = payload };

    // Standard control payload deserialization helpers
    public bool TryGetPayload<TPayload>(out TPayload? payload) where TPayload : class, IResilientPayload
    {
        payload = null;
        if (Payload.IsEmpty) return false;

        var reader = new SequenceReader<byte>(Payload);
        if (typeof(TPayload) == typeof(ConnectPacketPayload))
        {
            if (ConnectPacketPayload.TryRead(ref reader, out var connectPayload))
            {
                payload = connectPayload as TPayload;
                return payload != null;
            }
        }
        else if (typeof(TPayload) == typeof(DisconnectPacketPayload))
        {
            if (DisconnectPacketPayload.TryRead(ref reader, out var disconnectPayload))
            {
                payload = disconnectPayload as TPayload;
                return payload != null;
            }
        }
        else if (typeof(TPayload) == typeof(AuthenticatePacketPayload))
        {
            if (AuthenticatePacketPayload.TryRead(ref reader, out var authPayload))
            {
                payload = authPayload as TPayload;
                return payload != null;
            }
        }

        return false;
    }
}
```

---

## 3. Option B: Source-Generated Framing

To avoid writing low-level parsing methods manually (which can be error-prone and tedious), decorate a partial struct with the `[GenerateFramingProtocol]` attribute. The generator automatically implements the `TryRead`, `TryWrite`, `GetEncodedLength`, and `WriteTo` methods based on the sequential `Order` of your decorated properties.

### Example Using Source Generator:

```csharp
using System.Buffers;
using Beskar.Memory.Flags;
using Beskar.Networking.Protocol.Attributes;
using Beskar.Networking.Protocol.Payloads;

namespace MyCustomNamespace;

[GenerateFramingProtocol]
public partial struct CustomAutoPacket
{
    // 1. Declare magic bytes to identify frames
    [MagicBytes(0xAB, 0xCD, Order = 0)]
    public partial bool HasValidMagicBytes { get; }

    // 2. Protocol version field (1 byte)
    [VersionField(Order = 1)]
    public byte Version { get; set; }

    // 3. Frame classification type
    [ProtocolField(Order = 2)]
    public MyPacketType PacketType { get; set; }

    // 4. Compact boolean status flags (2 bytes space)
    [FlagsField(Order = 3)]
    public PackedBools16 Flags { get; set; }

    // 5. Length prefix (encoded as variable length number)
    [VarNumberField(Order = 4)]
    public int PayloadLength { get; set; }

    // 6. Payload buffer mapping (linked to PayloadLength)
    [ByteSequenceField(nameof(PayloadLength), safeCopyData: false, Order = 5)]
    public ReadOnlySequence<byte> Payload { get; set; }
}
```

---

## 4. Bootstrapping Custom Framing

Once your custom frame structure is defined, initialize your server and client using the generic type argument:

### Server Initialization:
```csharp
var server = ResilientServerFactory.CreateBuilder<CustomMagicPacket>()
    .UseTcp(8000)
    .Build();

server.Events.FrameReceived.Add((ctx, ct) =>
{
    // ctx.Frame is of type CustomMagicPacket
    CustomMagicPacket frame = ctx.Frame; 
    Console.WriteLine($"Received custom frame of kind {frame.Kind}");
    return ValueTask.CompletedTask;
});

await server.StartAsync();
```

### Client Initialization:
```csharp
var client = ResilientClientFactory.CreateTcp<CustomMagicPacket>();

await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 8000));

var myData = "Custom payload"u8.ToArray();
var frame = CustomMagicPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(myData));

await client.SendAsync(frame);
```
