using System.IO.Pipelines;
using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

/// <summary>
/// Represents a TCP network session wrapping a single transport connection.
/// </summary>
public sealed class TcpNetworkSession(
   EndPoint localAddress,
   EndPoint remoteAddress,
   IDuplexPipe connection,
   Func<IDuplexPipe, ValueTask>? onDisposeAsync = null)
   : INetworkSession, IAsyncDisposable
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress { get; } = remoteAddress;
   public EndPoint LocalAddress { get; } = localAddress;

   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;

   public CancellationToken SessionClosedToken => _cts.Token;

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
         _stream = new TcpNetworkStream(this, _connection);
         return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
      }

      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(
         new NetworkCodeError(-1, "TCP session only supports a single stream, which has already been accepted."));
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      if (_stream is null)
      {
         _stream = new TcpNetworkStream(this, _connection);
         return new ValueTask<Result<INetworkStream, NetworkCodeError>>(_stream);
      }

      return new ValueTask<Result<INetworkStream, NetworkCodeError>>(
         new NetworkCodeError(-1, "TCP session only supports a single stream, which has already been opened."));
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }

      _cts.Dispose();

      if (onDisposeAsync is not null)
      {
         await onDisposeAsync(_connection);
      }
   }
}
