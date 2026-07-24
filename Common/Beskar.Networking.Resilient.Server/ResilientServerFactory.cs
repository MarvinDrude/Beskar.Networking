namespace Beskar.Networking.Resilient.Server;

/// <summary>
/// A factory class for creating Resilient Server builders.
/// </summary>
public static class ResilientServerFactory
{
   /// <summary>
   /// Creates a new instance of the <see cref="ResilientServerBuilder"/> class used to configure and build a resilient server.
   /// </summary>
   /// <param name="options">
   /// An optional <see cref="ResilientServerOptions"/> instance to configure the server builder.
   /// If null, a default set of options will be applied.
   /// </param>
   /// <returns>
   /// Returns an instance of <see cref="ResilientServerBuilder"/> configured with the provided or default options.
   /// </returns>
   public static ResilientServerBuilder CreateBuilder(ResilientServerOptions? options = null)
   {
      options ??= new ResilientServerOptions();
      return new ResilientServerBuilder(options);
   }
}
