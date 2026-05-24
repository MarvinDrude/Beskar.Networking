using System.Buffers;
using System.IO.Pipelines;
using Beskar.Networking.Transports.Common.Streams;
using TUnit.Assertions;

namespace Beskar.Networking.Transports.Common.Tests;

public class StreamConnectionTests
{
   [Test]
   public async Task Initialize_WithNullStream_ThrowsArgumentNullException()
   {
      var connection = new StreamConnection(PipeOptions.Default, PipeOptions.Default);
      
      await Assert.That(() => connection.Initialize(null!))
         .Throws<ArgumentNullException>();
   }

   [Test]
   public async Task Start_WithoutInitialize_ThrowsInvalidOperationException()
   {
      var connection = new StreamConnection(PipeOptions.Default, PipeOptions.Default);
      
      await Assert.That(() => connection.Start())
         .Throws<InvalidOperationException>();
   }

   [Test]
   public async Task Lifecycle_InitializeStartStop_Succeeds()
   {
      var connection = new StreamConnection(PipeOptions.Default, PipeOptions.Default);
      using var readStream = new MemoryStream();
      using var writeStream = new MemoryStream();
      using var duplexStream = new DuplexStream(readStream, writeStream);

      connection.Initialize(duplexStream);
      connection.Start();
      
      await connection.StopAsync();
      
      await Assert.That(connection.TryResetState()).IsTrue();
   }

   [Test]
   public async Task ReadWriteData_ThroughConnection_Succeeds()
   {
      var connection = new StreamConnection(PipeOptions.Default, PipeOptions.Default);
      
      // We will simulate a stream we write to, which acts as input for the StreamConnection.
      // And a stream that the StreamConnection writes to, which we read from.
      var incomingData = new MemoryStream();
      var outgoingData = new MemoryStream();
      
      var testPayload = "Hello from Beskar transport!"u8.ToArray();
      incomingData.Write(testPayload);
      incomingData.Position = 0; // Reset position so StreamConnection can read it

      var duplexStream = new DuplexStream(incomingData, outgoingData);
      connection.Initialize(duplexStream);
      connection.Start();

      // Read from connection input (which should pull from incomingData)
      var readResult = await connection.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      connection.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(testPayload);

      // Write to connection output (which should copy to outgoingData)
      await connection.Output.WriteAsync(testPayload);
      await connection.Output.FlushAsync();

      // Wait a short bit for the background copy task to write to outgoingData
      await Task.Delay(100);

      var outgoingBytes = outgoingData.ToArray();
      await Assert.That(outgoingBytes).IsEquivalentTo(testPayload);

      await connection.StopAsync();
   }
}

public sealed class DuplexStream : Stream
{
   private readonly Stream _readStream;
   private readonly Stream _writeStream;

   public DuplexStream(Stream readStream, Stream writeStream)
   {
      _readStream = readStream;
      _writeStream = writeStream;
   }

   public override bool CanRead => _readStream.CanRead;
   public override bool CanSeek => false;
   public override bool CanWrite => _writeStream.CanWrite;
   public override long Length => throw new NotSupportedException();
   public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

   public override void Flush() => _writeStream.Flush();
   public override Task FlushAsync(CancellationToken cancellationToken) => _writeStream.FlushAsync(cancellationToken);

   public override int Read(byte[] buffer, int offset, int count) => _readStream.Read(buffer, offset, count);
   public override int Read(Span<byte> buffer) => _readStream.Read(buffer);
   public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _readStream.ReadAsync(buffer, cancellationToken);
   public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _readStream.ReadAsync(buffer, offset, count, cancellationToken);

   public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
   public override void SetLength(long value) => throw new NotSupportedException();

   public override void Write(byte[] buffer, int offset, int count) => _writeStream.Write(buffer, offset, count);
   public override void Write(ReadOnlySpan<byte> buffer) => _writeStream.Write(buffer);
   public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _writeStream.WriteAsync(buffer, cancellationToken);
   public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _writeStream.WriteAsync(buffer, offset, count, cancellationToken);

   protected override void Dispose(bool disposing)
   {
      if (disposing)
      {
         _readStream.Dispose();
         _writeStream.Dispose();
      }
      base.Dispose(disposing);
   }

   public override async ValueTask DisposeAsync()
   {
      await _readStream.DisposeAsync();
      await _writeStream.DisposeAsync();
      await base.DisposeAsync();
   }
}
