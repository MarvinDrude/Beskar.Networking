using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Netcode;

public readonly record struct PredictedStep(ClientInputFrame Input, float PredX, float PredY);

public readonly record struct PendingOptimisticPickup(OrbState2D Orb, double PredictedAtSec);

internal sealed class PredictedLaser
{
   public uint ShotSeq;

   public float X;
   public float Y;

   public float PrevX;
   public float PrevY;

   public float Vx;
   public float Vy;

   public byte Color { get; init; }
   public int LifeTicks;
   public bool ConfirmedByServer;
}

internal readonly record struct RecentLocalFx(float X, float Y, FxKind2D Kind, double TimeSec);

internal sealed class TimedSnapshot
{
   public required double ServerTimeSec { get; init; }
   public required double ReceivedTimeSec { get; init; }

   public required uint ServerTick { get; init; }

   public required List<PlayerState2D> Players { get; init; }
   public required List<OrbState2D> Orbs { get; init; }
   public required List<LaserState2D> Lasers { get; init; }
}
