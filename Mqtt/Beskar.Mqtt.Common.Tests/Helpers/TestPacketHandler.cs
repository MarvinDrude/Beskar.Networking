using System.Threading;
using System.Threading.Tasks;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Tests.Helpers;

public class TestPacketHandler : IPacketHandler
{
    public delegate void PubRecCallback(in PubRecPacket packet);
    public delegate void PubAckCallback(in PubAckPacket packet);
    public delegate void PubCompCallback(in PubCompPacket packet);
    public delegate void PubRelCallback(in PubRelPacket packet);
    public delegate void PublishCallback(in PublishPacket packet);
    public delegate void ConnectCallback(in ConnectPacket packet);
    public delegate void ConnAckCallback(in ConnAckPacket packet);
    public delegate void SubAckCallback(in SubAckPacket packet);
    public delegate void SubscribeCallback(in SubscribePacket packet);
    public delegate void UnsubAckCallback(in UnsubAckPacket packet);
    public delegate void UnsubscribeCallback(in UnsubscribePacket packet);
    public delegate void PingReqCallback(in PingReqPacket packet);
    public delegate void PingRespCallback(in PingRespPacket packet);
    public delegate void DisconnectCallback(in DisconnectPacket packet);
    public delegate void AuthCallback(in AuthPacket packet);

    public PubRecCallback? OnPubRec;
    public PubAckCallback? OnPubAck;
    public PubCompCallback? OnPubComp;
    public PubRelCallback? OnPubRel;
    public PublishCallback? OnPublish;
    public ConnectCallback? OnConnect;
    public ConnAckCallback? OnConnAck;
    public SubAckCallback? OnSubAck;
    public SubscribeCallback? OnSubscribe;
    public UnsubAckCallback? OnUnsubAck;
    public UnsubscribeCallback? OnUnsubscribe;
    public PingReqCallback? OnPingReq;
    public PingRespCallback? OnPingResp;
    public DisconnectCallback? OnDisconnect;
    public AuthCallback? OnAuth;

    public ValueTask ExecuteAsync(in PubRecPacket packet, CancellationToken ct = default)
    {
        OnPubRec?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in PubAckPacket packet, CancellationToken ct = default)
    {
        OnPubAck?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in PubCompPacket packet, CancellationToken ct = default)
    {
        OnPubComp?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in PubRelPacket packet, CancellationToken ct = default)
    {
        OnPubRel?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in PublishPacket packet, CancellationToken ct = default)
    {
        OnPublish?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in ConnectPacket packet, CancellationToken ct = default)
    {
        OnConnect?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in ConnAckPacket packet, CancellationToken ct = default)
    {
        OnConnAck?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in SubAckPacket packet, CancellationToken ct = default)
    {
        OnSubAck?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in SubscribePacket packet, CancellationToken ct = default)
    {
        OnSubscribe?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in UnsubAckPacket packet, CancellationToken ct = default)
    {
        OnUnsubAck?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in UnsubscribePacket packet, CancellationToken ct = default)
    {
        OnUnsubscribe?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in PingReqPacket packet, CancellationToken ct = default)
    {
        OnPingReq?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in PingRespPacket packet, CancellationToken ct = default)
    {
        OnPingResp?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in DisconnectPacket packet, CancellationToken ct = default)
    {
        OnDisconnect?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteAsync(in AuthPacket packet, CancellationToken ct = default)
    {
        OnAuth?.Invoke(in packet);
        return ValueTask.CompletedTask;
    }
}
