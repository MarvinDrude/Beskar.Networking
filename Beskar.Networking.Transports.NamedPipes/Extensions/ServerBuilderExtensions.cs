using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.NamedPipes.Extensions;

public static class ServerBuilderExtensions
{
   extension<TSelf>(IServerBuilder<TSelf> builder)
   {
      /// <summary>
      /// Configures the server to use a Named Pipes transport listener.
      /// </summary>
      /// <param name="endpoint">The <see cref="NamedPipeEndPoint"/> to bind the Named Pipes transport listener.</param>
      /// <param name="options">Optional Named Pipes transport configuration options. Defaults to a new instance of <see cref="NamedPipeTransportOptions"/> if not provided.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseNamedPipes(NamedPipeEndPoint endpoint, NamedPipeTransportOptions? options = null)
      {
         options ??= new NamedPipeTransportOptions();
         return builder.Use(new NamedPipeNetworkListener(endpoint, options));
      }

      /// <summary>
      /// Configures the server to use a Named Pipes transport listener.
      /// </summary>
      /// <param name="pipeName">The name of the pipe to listen on.</param>
      /// <param name="options">Optional Named Pipes transport configuration options. Defaults to a new instance of <see cref="NamedPipeTransportOptions"/> if not provided.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseNamedPipes(string pipeName, NamedPipeTransportOptions? options = null)
      {
         return builder.UseNamedPipes(new NamedPipeEndPoint(pipeName), options);
      }
   }
}
