using System.IO.Pipelines;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// Represents a QUIC network session that supports stream multiplexing.
/// </summary>
public sealed class QuicNetworkSession(
   QuicConnection connection,
   QuicTransportOptions options,
   QuicIoQueueRegistry ioQueueRegistry)
   : INetworkSession
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress => _connection.RemoteEndPoint;
   public EndPoint LocalAddress => _connection.LocalEndPoint;

   public bool IsSupportingMultiplexing => true;
   public bool IsSupportingUnidirectional => true;

   public CancellationToken SessionClosedToken => _cts.Token;

   private readonly QuicConnection _connection = connection;
   private readonly QuicTransportOptions _options = options;
   private readonly QuicIoQueueRegistry _ioQueueRegistry = ioQueueRegistry;
   private readonly CancellationTokenSource _cts = new();

   private int _disposed;

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogServerInfo("QUIC Session {0}: Accepting incoming stream...", Id);
         using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
         var quicStream = await _connection.AcceptInboundStreamAsync(linkedCts.Token);

         var connection = _ioQueueRegistry.Create(quicStream);

         var newStream = new QuicNetworkStream(this, quicStream, connection);
         TraceLogger.LogServerInfo("QUIC Session {0}: Successfully accepted inbound {1} stream {2}", Id, newStream.Direction, newStream.StreamId);
         return newStream;
      }
      catch (QuicException ex)
      {
         if (ex.QuicError == QuicError.ConnectionAborted)
         {
            TraceLogger.LogServerInfo("QUIC Session {0}: Connection closed by peer gracefully or aborted (Application Error Code: {1})", Id, ex.ApplicationErrorCode);
         }
         else
         {
            TraceLogger.LogServerError("QUIC Session {0}: QuicException accepting inbound stream (Code: {1}): {2}", Id, (int)ex.QuicError, ex.Message);
         }
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (OperationCanceledException) when (_cts.IsCancellationRequested)
      {
         TraceLogger.LogServerWarning("QUIC Session {0}: Stream acceptance cancelled because session is closing.", Id);
         return new NetworkCodeError(-1, "Session has been closed.");
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("QUIC Session {0}: Unexpected exception accepting inbound stream: {1}", Id, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogClientInfo("QUIC Session {0}: Opening outbound stream (Direction: {1})...", Id, direction);
         using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
         var quicStreamType = direction == NetworkStreamDirection.Bidirectional
            ? QuicStreamType.Bidirectional
            : QuicStreamType.Unidirectional;

         var quicStream = await _connection.OpenOutboundStreamAsync(quicStreamType, linkedCts.Token);
         var connection = _ioQueueRegistry.Create(quicStream);

         var newStream = new QuicNetworkStream(this, quicStream, connection);
         TraceLogger.LogClientInfo("QUIC Session {0}: Successfully opened outbound {1} stream {2}", Id, newStream.Direction, newStream.StreamId);
         return newStream;
      }
      catch (QuicException ex)
      {
         TraceLogger.LogClientError("QUIC Session {0}: QuicException opening outbound stream (Code: {1}): {2}", Id, (int)ex.QuicError, ex.Message);
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (OperationCanceledException) when (_cts.IsCancellationRequested)
      {
         TraceLogger.LogClientWarning("QUIC Session {0}: Stream opening cancelled because session is closing.", Id);
         return new NetworkCodeError(-1, "Session has been closed.");
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("QUIC Session {0}: Unexpected exception opening outbound stream: {1}", Id, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   /// <summary>
   /// Returns a retired stream connection adapter to the shared connection pool.
   /// </summary>
   public async ValueTask ReturnConnectionAsync(StreamConnection connection)
   {
      TraceLogger.LogNeutralInfo("QUIC Session {0}: Returning stream connection to registry pool", Id);
      await _ioQueueRegistry.ReturnAsync(connection);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      TraceLogger.LogNeutralInfo("QUIC Session: Disposing and shutting down active session {0} (Remote: {1}, Local: {2})", Id, RemoteAddress, LocalAddress);

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }
      _cts.Dispose();

      try
      {
         // ReSharper disable once MethodSupportsCancellation
         await _connection.CloseAsync(_options.DefaultCloseErrorCode);
      }
      catch (Exception ex)
      {
         TraceLogger.LogNeutralWarning("QUIC Session {0}: Error closing connection: {1}", Id, ex.Message);
      }

      await _connection.DisposeAsync();
   }
}
