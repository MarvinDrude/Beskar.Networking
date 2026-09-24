using System.Net;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Ws.Benchmark;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var port = args.Length > 0 && int.TryParse(args[0], out var p) ? p : 8080;
        var options = new WsTransportOptions
        {
            Path = "/ws",
            KeepAliveInterval = TimeSpan.Zero,
            OnMessageAsync = (session, payload, opcode) => session.SendFrameAsync(payload, opcode)
        };
        options.TcpOptions.SocketOptions.IoQueueCount = Environment.ProcessorCount;

        var endPoint = new IPEndPoint(IPAddress.Any, port);
        var listener = new WsNetworkListener(endPoint, options);

        var bindResult = await listener.BindAsync();
        if (bindResult.Failed)
        {
            Console.WriteLine($"Failed to bind WebSocket listener: {bindResult.Error.Message}");
            return;
        }

        Console.WriteLine($"Beskar WebSocket server listening on port {port} (Path: /ws)...");
        await Task.Delay(-1);
    }
}
