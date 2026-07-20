using System.Buffers;
using Beskar.Networking.Transports.NamedPipes;

namespace Beskar.Networking.Transports.NamedPipes.Tests;

public class  NamedPipeTransportTests
{
   private static string GetTempPipeName()
   {
      return $"beskar-test-pipe-{Guid.NewGuid():N}";
   }

   [Test]
   public async Task NamedPipeClientServer_StandardConnection_DataExchangedSuccessfully()
   {
      var pipeName = GetTempPipeName();
      var endPoint = new NamedPipeEndPoint(pipeName);
      var options = new NamedPipeTransportOptions();
      var listener = new NamedPipeNetworkListener(endPoint, options);

      // Assert initially unbound
      await Assert.That(listener.IsBound).IsFalse();

      // Bind listener
      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();
      await Assert.That(listener.IsBound).IsTrue();
      await Assert.That(listener.Stats.Binds).IsEqualTo(1);

      // Connect client
      var client = new NamedPipeNetworkClient(options);
      await Assert.That(client.IsConnected).IsFalse();
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(client.Stats.ConnectionsEstablished).IsEqualTo(1);

      // Accept server session
      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      await Assert.That(listener.Stats.SessionsAccepted).IsEqualTo(1);

      // Assert session security info
      await Assert.That(clientSession.SecurityInfo.IsEncrypted).IsFalse();
      await Assert.That(serverSession.SecurityInfo.IsEncrypted).IsFalse();

      // Accept / Open streams
      var clientStreamResult = await clientSession.OpenStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      await Assert.That(clientStreamResult.Failed).IsFalse();
      await Assert.That(serverStreamResult.Failed).IsFalse();

      await Assert.That(clientSession.SessionStats.StreamsOpened).IsEqualTo(1);
      await Assert.That(serverSession.SessionStats.StreamsAccepted).IsEqualTo(1);

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      // Client writes to server
      var payload = "Hello from Pipe Client!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      // Server reads from client
      var readResult = await serverStream.Transport.Input.ReadAsync();
      var readBytes = readResult.Buffer.ToArray();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(readBytes).IsEquivalentTo(payload);

      // Server writes to client
      var serverPayload = "Hello from Pipe Server!"u8.ToArray();
      await serverStream.Transport.Output.WriteAsync(serverPayload);
      await serverStream.Transport.Output.FlushAsync();

      // Client reads from server
      var clientReadResult = await clientStream.Transport.Input.ReadAsync();
      var clientReadBytes = clientReadResult.Buffer.ToArray();
      clientStream.Transport.Input.AdvanceTo(clientReadResult.Buffer.End);

      await Assert.That(clientReadBytes).IsEquivalentTo(serverPayload);

      // Cleanup
      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();

      await Assert.That(client.Stats.ConnectionsLost).IsEqualTo(1);
      await Assert.That(listener.Stats.Unbinds).IsEqualTo(1);
   }

   [Test]
   public async Task NamedPipeClient_DisconnectAsync_ClosesSessionAndCancelsSessionClosedToken()
   {
      var pipeName = GetTempPipeName();
      var endPoint = new NamedPipeEndPoint(pipeName);
      var options = new NamedPipeTransportOptions();
      var listener = new NamedPipeNetworkListener(endPoint, options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new NamedPipeNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var sessionClosedToken = clientSession.SessionClosedToken;

      await Assert.That(sessionClosedToken.IsCancellationRequested).IsFalse();

      await client.DisconnectAsync(sessionClosedToken);

      await Assert.That(sessionClosedToken.IsCancellationRequested).IsTrue();

      await listener.UnbindAsync(sessionClosedToken);
   }

   [Test]
   public async Task NamedPipeClientServer_StandardConnection_StatsTrackedCorrectly()
   {
      var pipeName = GetTempPipeName();
      var endPoint = new NamedPipeEndPoint(pipeName);
      var options = new NamedPipeTransportOptions();
      var listener = new NamedPipeNetworkListener(endPoint, options);

      var bindResult = await listener.BindAsync();
      await Assert.That(bindResult.Failed).IsFalse();

      var client = new NamedPipeNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      await Assert.That(connectResult.Failed).IsFalse();

      var acceptResult = await listener.AcceptSessionAsync();
      await Assert.That(acceptResult.Failed).IsFalse();

      var clientSession = connectResult.Success!;
      var serverSession = acceptResult.Success!;

      var clientStreamResult = await clientSession.AcceptStreamAsync();
      var serverStreamResult = await serverSession.AcceptStreamAsync();

      await Assert.That(clientStreamResult.Failed).IsFalse();
      await Assert.That(serverStreamResult.Failed).IsFalse();

      var clientStream = clientStreamResult.Success!;
      var serverStream = serverStreamResult.Success!;

      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(0);
      await Assert.That(clientStream.Stats.BytesReceived).IsEqualTo(0);

      var payload = "Hello Pipe Stats"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var readResult = await serverStream.Transport.Input.ReadAsync();
      serverStream.Transport.Input.AdvanceTo(readResult.Buffer.End);

      await Assert.That(clientStream.Stats.BytesSent).IsEqualTo(payload.Length);
      await Assert.That(clientSession.Stats.BytesSent).IsEqualTo(payload.Length);
      await Assert.That(serverStream.Stats.BytesReceived).IsEqualTo(payload.Length);
      await Assert.That(serverSession.Stats.BytesReceived).IsEqualTo(payload.Length);

      await clientSession.DisposeAsync();
      await serverSession.DisposeAsync();
      await listener.UnbindAsync();
   }

   [Test]
   public async Task NamedPipeListener_BindUnbindBindUnbind_SuccessiveCallsWork()
   {
      var pipeName = GetTempPipeName();
      var endPoint = new NamedPipeEndPoint(pipeName);
      var options = new NamedPipeTransportOptions();
      var listener = new NamedPipeNetworkListener(endPoint, options);

      for (var i = 0; i < 3; i++)
      {
         var bindResult = await listener.BindAsync();
         await Assert.That(bindResult.Failed).IsFalse();
         await Assert.That(listener.IsBound).IsTrue();

         await listener.UnbindAsync();
         await Assert.That(listener.IsBound).IsFalse();
      }
   }
}
