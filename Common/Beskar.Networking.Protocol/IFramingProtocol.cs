using System.Buffers;
using Beskar.Networking.Protocol.Payloads;

namespace Beskar.Networking.Protocol;

/// <summary>
/// Defines the contract for a high-performance framing protocol struct or type.
/// </summary>
/// <typeparam name="TSelf">The implementing framing protocol type.</typeparam>
public interface IFramingProtocol<TSelf> where TSelf : struct, IFramingProtocol<TSelf>
{
   /// <summary>
   /// Gets the encoded byte length of this protocol frame.
   /// </summary>
   int GetEncodedLength();

   /// <summary>
   /// Attempts to write this protocol frame into a destination span.
   /// </summary>
   bool TryWrite(Span<byte> destination, out int bytesWritten);

   /// <summary>
   /// Writes this protocol frame into an IBufferWriter.
   /// </summary>
   void WriteTo(IBufferWriter<byte> writer);

   /// <summary>
   /// Attempts to read a protocol frame from a SequenceReader.
   /// </summary>
   static abstract bool TryRead(ref SequenceReader<byte> reader, out TSelf result);

   /// <summary>
   /// Gets the classification kind of this frame (Connect, Disconnect, Ping, Pong, Message, etc.).
   /// Defaults to ResilientFrameKind.Message.
   /// </summary>
   ResilientFrameKind GetFrameKind() => ResilientFrameKind.Message;

   /// <summary>
   /// Optionally creates a protocol frame instance of the specified kind.
   /// </summary>
   static virtual TSelf CreateFrame(ResilientFrameKind kind) => default;

   /// <summary>
   /// Optionally creates a protocol frame instance of the specified kind with a payload sequence.
   /// </summary>
   static virtual TSelf CreateFrame(ResilientFrameKind kind, ReadOnlySequence<byte> payload) => TSelf.CreateFrame(kind);

   /// <summary>
   /// Gets the payload byte sequence of this frame if available.
   /// Defaults to empty sequence.
   /// </summary>
   ReadOnlySequence<byte> GetPayloadSequence() => ReadOnlySequence<byte>.Empty;

   /// <summary>
   /// Attempts to deserialize or extract a control payload out of the frame.
   /// </summary>
   bool TryGetPayload<TPayload>(out TPayload? payload) where TPayload : class, IResilientPayload
   {
      payload = null;
      return false;
   }
}
