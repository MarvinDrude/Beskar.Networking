using System.IO.Pipelines;
using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

/// <summary>
/// Represents a TCP network session wrapping a single transport connection.
/// </summary>
public sealed class TcpNetworkSession(
   EndPoint localAddress,
   EndPoint remoteAddress,
   IDuplexPipe connection,
   Func<IDuplexPipe, ValueTask>? onDisposeAsync = null)
   : INetworkSession
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress { get; } = remoteAddress;
   public EndPoint LocalAddress { get; } = localAddress;

   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;

   public CancellationToken SessionClosedToken => _cts.Token;

   public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();

   public NetworkStats Stats => _stream?.Stats ?? new NetworkStats();

   private readonly IDuplexPipe _connection = connection;
   private readonly CancellationTokenSource _cts = new();

   private TcpNetworkStream? _stream;
   private int _disposed;

   /// <inheritdoc />
   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(
      CancellationToken ct = default)
   {
      if (_stream is null)
      {
         TraceLogger.LogServerInfo("TCP Session {0}: Instantiating bidirectional TCP stream wrapper", Id);
         _stream = new TcpNetworkStream(this, _connection);
      }

      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      if (_stream is null)
      {
         TraceLogger.LogClientInfo("TCP Session {0}: Opening bidirectional TCP stream connection", Id);
         _stream = new TcpNetworkStream(this, _connection);
      }

      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      var origin = onDisposeAsync is not null ? TraceLogOrigin.Server : TraceLogOrigin.Client;
      TraceLogger.LogInfo("Disposing and shutting down active TCP session {0}", origin, Id);

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }

      _cts.Dispose();

      if (_stream is not null)
      {
         try
         {
            await _stream.DisposeAsync();
         }
         catch
         {
            // Ignored
         }
      }

      if (onDisposeAsync is not null)
      {
         await onDisposeAsync(_connection);
      }
   }
}
