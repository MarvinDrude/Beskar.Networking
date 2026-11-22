namespace Beskar.Datagram.Example.ArenaGame2D.Protocol;

[Flags]
public enum InputButtons : byte
{
   None = 0,
   Up = 1 << 0,
   Down = 1 << 1,
   Left = 1 << 2,
   Right = 1 << 3,
   Fire = 1 << 4
}

public enum FxKind2D : byte
{
   OrbPickup = 0,
   PlayerTag = 1,
   LaserShot = 2,
   HitSpark = 3
}

public readonly record struct ArenaObstacle(float X, float Y, float W, float H);

public readonly record struct ClientInputFrame(
   uint InputSeq,
   InputButtons Buttons,
   float AimAngle,
   float ViewServerTick = 0f);

public readonly record struct PlayerState2D(
   byte Id,
   float X,
   float Y,
   float FacingAngle,
   byte Color,
   bool Stunned,
   bool IsBot);

public readonly record struct OrbState2D(byte Id, float X, float Y, byte Value);

public readonly record struct LaserState2D(
   float X,
   float Y,
   float Vx,
   float Vy,
   byte Color,
   byte OwnerId,
   uint ShotSeq);

public readonly record struct ScoreEntry2D(byte Id, byte Color, ushort Score, string Name);

public readonly record struct FxCue2D(float X, float Y, FxKind2D Kind, byte Color);
