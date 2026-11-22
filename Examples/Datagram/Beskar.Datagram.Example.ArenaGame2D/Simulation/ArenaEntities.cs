using Beskar.Datagram.Example.ArenaGame2D.Protocol;

namespace Beskar.Datagram.Example.ArenaGame2D.Simulation;

/// <summary>
/// Authoritative player entity tracked by the Host simulation.
/// </summary>
public sealed class PlayerEntity
{
   public byte Id { get; init; }
   public uint? PeerId { get; init; }

   public required string Name { get; init; }
   public byte Color { get; init; }

   public bool IsBot { get; init; }

   public float X;
   public float Y;
   public float FacingAngle;

   public float PrevX;
   public float PrevY;
   public float PrevFacingAngle;

   public ushort Score { get; set; }

   public uint LastProcessedInputSeq { get; set; }
   public int FireCooldownTicks { get; set; }
   public int StunTicks { get; set; }

   public Queue<ClientInputFrame> PendingInputs { get; } = new(32);

   public bool InputBufferPrimed { get; set; }
   public int EmptyInputTicks { get; set; }

   public float BotWanderAngle { get; set; }
   public int BotDecisionTimer { get; set; }
}

internal sealed class LaserEntity
{
   public byte OwnerId { get; init; }
   public uint ShotSeq { get; init; }
   public byte Color { get; init; }

   public float X;
   public float Y;

   public float PrevX;
   public float PrevY;

   public float Vx;
   public float Vy;

   public int LifeTicks;
   public float RewindOffsetTicks;
}

/// <summary>
/// Ephemeral visual record of a server-side lag-compensated hit (drawn in Host Debug Mode).
/// </summary>
public readonly record struct RewindHitMarker(
   byte TargetId,
   float RewoundX,
   float RewoundY,
   float LiveX,
   float LiveY,
   float RewindMs,
   int RemainingTicks);
