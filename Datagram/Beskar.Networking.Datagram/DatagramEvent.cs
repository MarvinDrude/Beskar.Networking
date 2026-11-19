using System.Net;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram;

/// <summary>
/// Event emitted by <see cref="DatagramHost"/> for peer lifecycle transitions and incoming channel messages.
/// Dispose after handling to return any pooled payload buffer to <see cref="System.Buffers.ArrayPool{T}.Shared"/>.
/// </summary>
public struct DatagramEvent : IDisposable
{
   public DatagramEventType Type { get; init; }
   public DatagramPeer Peer { get; init; }

   public byte ChannelId { get; init; }

   public DatagramDeliveryMode DeliveryMode { get; init; }
   public DatagramDisconnectReason DisconnectReason { get; init; }

   public IPEndPoint? PreviousEndPoint { get; init; }
   public IPEndPoint? NewEndPoint { get; init; }

   private DatagramPayloadLease _payload;

   public readonly DatagramPayloadLease Payload => _payload;

   internal static DatagramEvent PeerConnected(DatagramPeer peer) => new()
   {
      Type = DatagramEventType.PeerConnected,
      Peer = peer,

      _payload = DatagramPayloadLease.Empty
   };

   internal static DatagramEvent PeerDisconnected(
      DatagramPeer peer,
      DatagramDisconnectReason reason,
      DatagramPayloadLease reasonPayload) => new()
   {
      Type = DatagramEventType.PeerDisconnected,
      Peer = peer,
      DisconnectReason = reason,

      _payload = reasonPayload
   };

   internal static DatagramEvent MessageReceived(
      DatagramPeer peer,
      byte channelId,
      DatagramDeliveryMode mode,
      DatagramPayloadLease payload) => new()
   {
      Type = DatagramEventType.MessageReceived,
      Peer = peer,
      ChannelId = channelId,
      DeliveryMode = mode,

      _payload = payload
   };

   internal static DatagramEvent PeerMigrated(
      DatagramPeer peer,
      IPEndPoint previousEndPoint,
      IPEndPoint newEndPoint) => new()
   {
      Type = DatagramEventType.PeerMigrated,
      Peer = peer,
      PreviousEndPoint = previousEndPoint,
      NewEndPoint = newEndPoint,

      _payload = DatagramPayloadLease.Empty
   };

   public void Dispose()
   {
      _payload.Dispose();
   }
}
