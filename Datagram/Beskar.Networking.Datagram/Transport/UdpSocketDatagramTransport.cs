using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Networking.Datagram.Transport;

/// <summary>
/// High-performance OS UDP socket transport utilizing allocation-free <see cref="SocketAddress"/> I/O.
/// </summary>
public sealed class UdpSocketDatagramTransport : IDatagramTransport
{
   private Socket? _socket;
   private IPEndPoint? _localEndPoint;
   private int _disposed;

   public IPEndPoint? LocalEndPoint => _localEndPoint;

   public void Bind(IPEndPoint localEndPoint, DatagramOptions options)
   {
      if (_socket is not null)
      {
         throw new InvalidOperationException("Transport is already bound.");
      }

      var socket = new Socket(localEndPoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);

      if (OperatingSystem.IsWindows())
      {
         const int sioUdpConnreset = -1744830452;
         socket.IOControl(sioUdpConnreset, [.. "\0\0\0\0"u8], null);
      }

      if (localEndPoint.AddressFamily == AddressFamily.InterNetworkV6 && localEndPoint.Address.Equals(IPAddress.IPv6Any))
      {
         socket.DualMode = true;
      }

      if (options.SocketSendBufferSize.HasValue)
      {
         socket.SendBufferSize = options.SocketSendBufferSize.Value;
      }

      if (options.SocketReceiveBufferSize.HasValue)
      {
         socket.ReceiveBufferSize = options.SocketReceiveBufferSize.Value;
      }

      socket.Bind(localEndPoint);
      _socket = socket;
      _localEndPoint = socket.LocalEndPoint as IPEndPoint ?? localEndPoint;
   }

   public ValueTask<int> ReceiveFromAsync(Memory<byte> buffer, SocketAddress remoteAddress, CancellationToken ct)
   {
      var socket = _socket ?? throw new ObjectDisposedException(nameof(UdpSocketDatagramTransport));
      return socket.ReceiveFromAsync(buffer, SocketFlags.None, remoteAddress, ct);
   }

   public async ValueTask SendToAsync(ReadOnlyMemory<byte> datagram, SocketAddress remoteAddress, CancellationToken ct)
   {
      var socket = _socket;
      if (socket is null || Volatile.Read(ref _disposed) == 1)
      {
         return;
      }

      await socket.SendToAsync(datagram, SocketFlags.None, remoteAddress, ct);
   }

   public ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return ValueTask.CompletedTask;
      }

      var socket = Interlocked.Exchange(ref _socket, null);
      if (socket is not null)
      {
         try
         {
            socket.Close();
         }
         catch
         {
            // Ignore errors on socket close
         }
         socket.Dispose();
      }

      return ValueTask.CompletedTask;
   }
}
