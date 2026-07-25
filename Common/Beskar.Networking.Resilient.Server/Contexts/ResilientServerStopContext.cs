using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Server.Contexts;

/// <summary>
/// Context passed when the ResilientServer stops.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientServerStopContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The resilient server instance.
   /// </summary>
   public required ResilientServer<TFrame> Server { get; init; }
}
