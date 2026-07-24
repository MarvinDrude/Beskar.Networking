using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Server.Contexts;

/// <summary>
/// Context passed when the ResilientServer starts.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientServerStartContext<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// The resilient server instance.
   /// </summary>
   public required ResilientServer<TFrame> Server { get; init; }
}
