using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol;
using Beskar.Networking.Transports.Memory;
using Beskar.Networking.Transports.NamedPipes;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Udp;
using Beskar.Networking.Transports.Uds;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Networking.Resilient.Client;

/// <summary>
/// A factory for creating instances of <see cref="ResilientClient{TFrame}"/> configured with various network transports.
/// </summary>
public abstract class ResilientClientFactory
{
   protected ResilientClientFactory()
   {
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance wrapping any custom <see cref="INetworkClient"/>.
   /// </summary>
   public static ResilientClient<TFrame> Create<TFrame>(
      INetworkClient networkClient,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      return new ResilientClient<TFrame>(networkClient, clientOptions);
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance communicating over TCP.
   /// </summary>
   public static ResilientClient<TFrame> CreateTcp<TFrame>(
      TcpTransportOptions? transportOptions = null,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      transportOptions ??= new TcpTransportOptions();
      transportOptions.StreamOptions.IoQueueCount = 1;
      transportOptions.SocketOptions.IoQueueCount = 1;

      var underlying = new TcpNetworkClient(transportOptions);
      return new ResilientClient<TFrame>(underlying, clientOptions);
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance communicating over WebSockets.
   /// </summary>
   public static ResilientClient<TFrame> CreateWs<TFrame>(
      WsTransportOptions? transportOptions = null,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      transportOptions ??= new WsTransportOptions();
      transportOptions.TcpOptions.StreamOptions.IoQueueCount = 1;
      transportOptions.TcpOptions.SocketOptions.IoQueueCount = 1;

      var underlying = new WsNetworkClient(transportOptions);
      return new ResilientClient<TFrame>(underlying, clientOptions);
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance communicating over QUIC.
   /// </summary>
   public static ResilientClient<TFrame> CreateQuic<TFrame>(
      QuicTransportOptions? transportOptions = null,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      transportOptions ??= new QuicTransportOptions();
      transportOptions.StreamOptions.IoQueueCount = 1;

      var underlying = new QuicNetworkClient(transportOptions);
      return new ResilientClient<TFrame>(underlying, clientOptions);
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance communicating over Named Pipes.
   /// </summary>
   public static ResilientClient<TFrame> CreateNamedPipes<TFrame>(
      NamedPipeTransportOptions? transportOptions = null,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      transportOptions ??= new NamedPipeTransportOptions();
      var underlying = new NamedPipeNetworkClient(transportOptions);
      return new ResilientClient<TFrame>(underlying, clientOptions);
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance communicating over Unix Domain Sockets (UDS).
   /// </summary>
   public static ResilientClient<TFrame> CreateUds<TFrame>(
      UdsTransportOptions? transportOptions = null,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      transportOptions ??= new UdsTransportOptions();
      var underlying = new UdsNetworkClient(transportOptions);
      return new ResilientClient<TFrame>(underlying, clientOptions);
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance communicating in-memory.
   /// </summary>
   public static ResilientClient<TFrame> CreateMemory<TFrame>(
      MemoryTransportOptions? transportOptions = null,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      transportOptions ??= new MemoryTransportOptions();
      var underlying = new MemoryNetworkClient(transportOptions);
      return new ResilientClient<TFrame>(underlying, clientOptions);
   }

   /// <summary>
   /// Creates a <see cref="ResilientClient{TFrame}"/> instance communicating over UDP.
   /// </summary>
   public static ResilientClient<TFrame> CreateUdp<TFrame>(
      UdpTransportOptions? transportOptions = null,
      ResilientClientOptions? clientOptions = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      transportOptions ??= new UdpTransportOptions();
      var underlying = new UdpNetworkClient(transportOptions);
      return new ResilientClient<TFrame>(underlying, clientOptions);
   }
}
