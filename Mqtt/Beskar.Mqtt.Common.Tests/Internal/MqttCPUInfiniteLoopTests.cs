using System.Net;
using System.Reflection;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttCPUInfiniteLoopTests
{
   [Test]
   public async Task MqttClient_ShouldNotLoopInfinitely_WhenClosedWithPartialData()
   {
      TraceLogger.IsEnabled = true;

      // Start server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create client
      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         CleanSession = true,
         ClientIdUtf8Bytes = "cpu-bug-client"u8.ToArray(),
         KeepAlivePeriod = 60
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Find the server-side client connection
      using var clientsResult = await server.ClientSessions.GetClients();
      var serverClient = clientsResult.WrittenSpan[0];

      // Write 1 byte of incomplete packet (0x10 is CONNECT, but we need remaining length)
      var output = serverClient.Stream.Transport.Output;
      var memory = output.GetMemory(1);
      memory.Span[0] = 0x10;
      output.Advance(1);
      await output.FlushAsync();

      // Complete output (closes connection abruptly with partial bytes in pipe buffer)
      await output.CompleteAsync();
      await serverClient.Session.DisposeAsync();

      // Wait a moment for client to process and verify it transitions to disconnected.
      // If the bug is present, client will spin forever and state will not change to 1 (Disconnected)
      var stateField = typeof(MqttClient).GetField("_state", BindingFlags.NonPublic | BindingFlags.Instance);
      
      var timeout = DateTimeOffset.UtcNow.AddSeconds(3);
      var disconnected = false;
      while (DateTimeOffset.UtcNow < timeout)
      {
         var state = (int)stateField!.GetValue(client)!;
         Console.WriteLine($"[TEST-DEBUG] Current client state = {state}");
         if (state == 1) // 1 = Disconnected
         {
            disconnected = true;
            break;
         }
         await Task.Delay(100);
      }

      await Assert.That(disconnected).IsTrue();
   }

   [Test]
   public async Task MqttServer_ShouldNotLoopInfinitely_WhenClosedWithPartialData()
   {
      // Start server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(0)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var port = ((IPEndPoint)server.Listeners[0].LocalAddress).Port;

      // Create client
      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var connectOptions = new ConnectOptions
      {
         EndPoint = new IPEndPoint(IPAddress.Loopback, port),
         CleanSession = true,
         ClientIdUtf8Bytes = "cpu-bug-server"u8.ToArray(),
         KeepAlivePeriod = 60
      };

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Verify server registered the client
      using (var clientsResult = await server.ClientSessions.GetClients())
      {
         await Assert.That(clientsResult.WrittenSpan.Length).IsEqualTo(1);
      }

      // Get client's control stream via reflection to send partial data to server
      var controlStreamField = typeof(MqttClient).GetField("_controlStream", BindingFlags.NonPublic | BindingFlags.Instance);
      var controlStream = (INetworkStream)controlStreamField!.GetValue(client)!;

      // Send incomplete data (1 byte)
      var output = controlStream.Transport.Output;
      var memory = output.GetMemory(1);
      memory.Span[0] = 0x10;
      output.Advance(1);
      await output.FlushAsync();

      // Complete output (closes connection abruptly with partial bytes in pipe buffer)
      await output.CompleteAsync();

      // Get the client's network session to dispose it
      var sessionField = typeof(MqttClient).GetField("_networkSession", BindingFlags.NonPublic | BindingFlags.Instance);
      var networkSession = (INetworkSession)sessionField!.GetValue(client)!;
      await networkSession.DisposeAsync();

      // Wait for server to process and check that the client connection is cleaned up.
      // If the bug is present, the receiver loop will spin forever, keeping the client session active.
      var timeout = DateTimeOffset.UtcNow.AddSeconds(3);
      var cleanedUp = false;
      while (DateTimeOffset.UtcNow < timeout)
      {
         using var clientsResult = await server.ClientSessions.GetClients();
         if (clientsResult.WrittenSpan.Length == 0)
         {
            cleanedUp = true;
            break;
         }
         await Task.Delay(50);
      }

      await Assert.That(cleanedUp).IsTrue();
   }
}
