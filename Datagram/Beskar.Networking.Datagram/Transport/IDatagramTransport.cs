using System.Net;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Networking.Datagram.Transport;

/// <summary>
/// Abstracts physical UDP datagram I/O so <see cref="DatagramHost"/> can run over real OS sockets
/// or deterministic simulated chaos networks without allocations on the hot path.
/// </summary>
public interface IDatagramTransport : IAsyncDisposable
{
   /// <summary>
   /// Gets the bound local endpoint once <see cref="Bind"/> has been called.
   /// </summary>
   public IPEndPoint? LocalEndPoint { get; }

   /// <summary>
   /// Binds the underlying transport to <paramref name="localEndPoint"/>.
   /// </summary>
   public void Bind(IPEndPoint localEndPoint, DatagramOptions options);

   /// <summary>
   /// Receives the next datagram into <paramref name="buffer"/>, populating <paramref name="remoteAddress"/>.
   /// </summary>
   public ValueTask<int> ReceiveFromAsync(Memory<byte> buffer, SocketAddress remoteAddress, CancellationToken ct);

   /// <summary>
   /// Sends a single datagram to <paramref name="remoteAddress"/>.
   /// </summary>
   public ValueTask SendToAsync(ReadOnlyMemory<byte> datagram, SocketAddress remoteAddress, CancellationToken ct);
}
