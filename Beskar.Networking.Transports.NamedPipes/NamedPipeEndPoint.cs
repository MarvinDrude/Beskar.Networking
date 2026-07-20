using System.Net;

namespace Beskar.Networking.Transports.NamedPipes;

/// <summary>
/// Represents a Named Pipe endpoint with a pipe name and server name.
/// </summary>
public sealed class NamedPipeEndPoint : EndPoint
{
   public string PipeName { get; }
   public string ServerName { get; }

   public NamedPipeEndPoint(string pipeName, string serverName = ".")
   {
      ArgumentException.ThrowIfNullOrWhiteSpace(pipeName);
      ArgumentException.ThrowIfNullOrWhiteSpace(serverName);

      PipeName = pipeName;
      ServerName = serverName;
   }

   public override string ToString()
   {
      return $@"\\{ServerName}\pipe\{PipeName}";
   }
}
