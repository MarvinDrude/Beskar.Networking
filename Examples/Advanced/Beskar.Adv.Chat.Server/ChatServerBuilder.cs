using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;
using Beskar.Networking.Transports.Quic;

namespace Beskar.Adv.Chat.Server;

public sealed class ChatServerBuilder
{
   private readonly List<INetworkListener> _listeners = [];

   public ChatServerBuilder UseTcp(int port)
   {
      var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Any, port), new TcpTransportOptions
      {
         NoDelay = true,
         UseSsl = false
      });

      _listeners.Add(listener);
      return this;
   }

   public ChatServerBuilder UseWs(int port)
   {
      var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Any, port), new WsTransportOptions
      {
         TcpOptions = new TcpTransportOptions
         {
            NoDelay = true,
            UseSsl = false
         }
      });

      _listeners.Add(listener);
      return this;
   }

   public ChatServerBuilder UseQuic(int port)
   {
      var listener = new QuicNetworkListener(new IPEndPoint(IPAddress.Any, port), new QuicTransportOptions
      {
         AlpnProtocol = "beskar-chat"
      });

      _listeners.Add(listener);
      return this;
   }

   public ChatServer Build()
   {
      return new ChatServer([.. _listeners]);
   }
}
