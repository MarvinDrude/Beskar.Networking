namespace Beskar.Networking.Abstractions.Enums;

/// <summary>
/// Represents the direction of a network stream.
/// </summary>
public enum NetworkStreamDirection : byte
{
   /// <summary>
   /// Bidirectional stream.
   /// </summary>
   Bidirectional = 1,
   /// <summary>
   /// Unidirectional stream.
   /// </summary>
   Unidirectional
}