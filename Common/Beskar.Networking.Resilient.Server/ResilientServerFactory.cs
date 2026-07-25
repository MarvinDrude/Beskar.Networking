using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Frames;

namespace Beskar.Networking.Resilient.Server;

/// <summary>
/// A factory class for creating Resilient Server builders.
/// </summary>
public static class ResilientServerFactory
{
   /// <summary>
   /// Creates a new instance of the builder class used to configure and build a resilient server.
   /// </summary>
   /// <param name="options">
   /// An optional <see cref="ResilientServerOptions"/> instance to configure the server builder.
   /// If null, a default set of options will be applied.
   /// </param>
   /// <returns>
   /// Returns an instance of a builder configured with the provided or default options.
   /// </returns>
   public static ResilientServerBuilder<BeskarPacket> CreateBuilder(ResilientServerOptions? options = null)
   {
      options ??= new ResilientServerOptions();
      return new ResilientServerBuilder<BeskarPacket>(options);
   }

   /// <summary>
   /// Creates a new instance of the builder class used to configure and build a resilient server
   /// with a specified framing protocol type.
   /// </summary>
   /// <typeparam name="TFrame">
   /// The framing protocol type that implements <see cref="IFramingProtocol{TSelf}"/>.
   /// This type defines the protocol for framing messages that the server will use.
   /// </typeparam>
   /// <param name="options">
   /// An optional <see cref="ResilientServerOptions"/> instance to configure the server builder.
   /// If null, a default set of options will be applied.
   /// </param>
   /// <returns>
   /// Returns an instance of a builder configured with the provided or default options,
   /// allowing further customization for a resilient server of the specified protocol type.
   /// </returns>
   public static ResilientServerBuilder<TFrame> CreateBuilder<TFrame>(ResilientServerOptions? options = null)
      where TFrame : struct, IFramingProtocol<TFrame>
   {
      options ??= new ResilientServerOptions();
      return new ResilientServerBuilder<TFrame>(options);
   }
}
