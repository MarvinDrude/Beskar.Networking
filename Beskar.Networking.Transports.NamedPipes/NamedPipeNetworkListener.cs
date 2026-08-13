using System.IO.Pipelines;
using System.IO.Pipes;
using System.Net;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Telemetry;

namespace Beskar.Networking.Transports.NamedPipes;

/// <summary>
/// A high-performance Named Pipe listener that supports concurrent accepted connections
/// using a non-blocking background queue.
/// </summary>
public sealed class NamedPipeNetworkListener(
   EndPoint localAddress,
   NamedPipeTransportOptions options)
   : INetworkListener
{
   private readonly EndPoint _configuredLocalAddress = localAddress;
   public EndPoint LocalAddress => _configuredLocalAddress;

   public TransportKind Transport => TransportKind.NamedPipe;
   public bool IsBound => _acceptCts is not null;

   private long _binds;
   private long _unbinds;
   private long _sessionsAccepted;

   public NetworkListenerStats Stats => new()
   {
      Binds = Interlocked.Read(ref _binds),
      Unbinds = Interlocked.Read(ref _unbinds),
      SessionsAccepted = Interlocked.Read(ref _sessionsAccepted)
   };

   private readonly NamedPipeTransportOptions _options = options;
   private readonly NamedPipeIoQueueRegistry _ioQueueRegistry = new(options);

   private CancellationTokenSource? _acceptCts;
   private SemaphoreSlim? _handshakeSemaphore;

   private int _disposedState; // 0 = active, 1 = disposed

   private Channel<Result<INetworkSession, NetworkCodeError>> _sessionChannel =
      Channel.CreateBounded<Result<INetworkSession, NetworkCodeError>>(new BoundedChannelOptions(1024)
      {
         SingleWriter = false,
         SingleReader = true,
         FullMode = BoundedChannelFullMode.Wait
      });

   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      try
      {
         _sessionChannel = Channel.CreateBounded<Result<INetworkSession, NetworkCodeError>>(
            new BoundedChannelOptions(_options.MaxPendingConnections)
            {
               SingleWriter = false,
               SingleReader = true,
               FullMode = BoundedChannelFullMode.Wait
            });

         if (LocalAddress is not NamedPipeEndPoint namedPipeEndPoint)
         {
            return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, "LocalAddress must be a NamedPipeEndPoint."));
         }

         TraceLogger.LogServerInfo("NamedPipe Listener: Binding to Named Pipe {0}", namedPipeEndPoint.PipeName);

         _acceptCts = new CancellationTokenSource();
         _handshakeSemaphore = new SemaphoreSlim(_options.MaxConcurrentHandshakes);

         _ = AcceptLoopAsync(namedPipeEndPoint, _acceptCts.Token);
         TraceLogger.LogServerInfo("NamedPipe Listener: Successfully bound and waiting for connections on {0}", namedPipeEndPoint);
         Interlocked.Increment(ref _binds);
         TransportMetrics.RecordListenerStarted(TransportKind.NamedPipe);

         return new ValueTask<VoidResult<NetworkCodeError>>(true);
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("NamedPipe Listener: Failed to bind: {0}", ex.Message);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, ex.Message));
      }
   }

   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogServerInfo("NamedPipe Listener: Unbinding and stopping listener");
         if (_acceptCts is not null)
         {
            await _acceptCts.CancelAsync();

            _acceptCts.Dispose();
            _acceptCts = null;
         }

         var semaphore = Interlocked.Exchange(ref _handshakeSemaphore, null);
         semaphore?.Dispose();

         _sessionChannel.Writer.TryComplete();
         while (_sessionChannel.Reader.TryRead(out var result))
         {
            if (!result.Failed)
            {
               await result.Success.DisposeAsync();
            }
         }

         TraceLogger.LogServerInfo("NamedPipe Listener: Successfully unbound");
         Interlocked.Increment(ref _unbinds);
         TransportMetrics.RecordListenerStopped(TransportKind.NamedPipe);

         return true;
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("NamedPipe Listener: Error during unbind: {0}", ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      if (!IsBound)
      {
         return new NetworkCodeError(-1, "Listener is not bound. Call BindAsync first.");
      }

      try
      {
         return _sessionChannel.Reader.TryRead(out var result)
            ? result
            : await _sessionChannel.Reader.ReadAsync(ct);
      }
      catch (ChannelClosedException)
      {
         return new NetworkCodeError(-1, "Listener has been unbound and session channel is closed.");
      }
   }

   private async Task AcceptLoopAsync(NamedPipeEndPoint endPoint, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         var semaphore = _handshakeSemaphore;
         if (semaphore is null)
         {
            break;
         }

         try
         {
            await semaphore.WaitAsync(token);

            NamedPipeServerStream pipeServer;
            try
            {
               pipeServer = new NamedPipeServerStream(
                  endPoint.PipeName,
                  System.IO.Pipes.PipeDirection.InOut,
                  NamedPipeServerStream.MaxAllowedServerInstances,
                  PipeTransmissionMode.Byte,
                  System.IO.Pipes.PipeOptions.Asynchronous,
                  inBufferSize: _options.InputBufferSize,
                  outBufferSize: _options.OutputBufferSize
               );
            }
            catch (Exception ex)
            {
               semaphore.Release();
               TraceLogger.LogServerError("NamedPipe Listener: Failed to create NamedPipeServerStream: {0}", ex.Message);

               WriteToSessionChannel(new NetworkCodeError(-1, ex.Message));
               await Task.Delay(_options.AcceptExceptionDelay, token);

               continue;
            }

            try
            {
               await pipeServer.WaitForConnectionAsync(token);
               TraceLogger.LogServerInfo("NamedPipe Listener: Accepted connection from Named Pipe client");

               _ = Task.Run(async () =>
               {
                  try
                  {
                     await HandshakeAndEnqueueAsync(pipeServer, endPoint, token);
                  }
                  finally
                  {
                     semaphore.Release();
                  }
               }, token);
            }
            catch (Exception ex)
            {
               await pipeServer.DisposeAsync();
               semaphore.Release();

               if (token.IsCancellationRequested)
               {
                  break;
               }

               TraceLogger.LogServerError("NamedPipe Listener: Error waiting for connection: {0}", ex.Message);
               await Task.Delay(_options.AcceptExceptionDelay, token);
            }
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (ObjectDisposedException)
         {
            break;
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested)
            {
               break;
            }

            TraceLogger.LogServerError("NamedPipe Listener: Unexpected error accepting client: {0}", ex.Message);
            WriteToSessionChannel(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(_options.AcceptExceptionDelay, token); } catch (OperationCanceledException) { break; }
         }
      }
   }

   private async Task HandshakeAndEnqueueAsync(
      NamedPipeServerStream pipeServer,
      NamedPipeEndPoint endPoint,
      CancellationToken token)
   {
      IDuplexPipe? connection = null;
      var success = false;

      try
      {
         connection = _ioQueueRegistry.Create(pipeServer);
         var session = new NamedPipeNetworkSession(endPoint, endPoint, connection, _ioQueueRegistry.ReturnAsync);

         TraceLogger.LogServerInfo("NamedPipe Listener: Enqueuing network session {0}", session.Id);
         Interlocked.Increment(ref _sessionsAccepted);

         await _sessionChannel.Writer.WriteAsync(session, token);
         success = true;
      }
      catch (OperationCanceledException)
      {
         TraceLogger.LogServerError("NamedPipe Listener: Connection processing cancelled");
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("NamedPipe Listener: Unexpected error during connection setup: {0}", ex.Message);
         WriteToSessionChannel(new NetworkCodeError(-1, ex.Message));
      }
      finally
      {
         if (!success)
         {
            if (connection is not null)
            {
               await _ioQueueRegistry.ReturnAsync(connection);
            }
            else
            {
               pipeServer.Dispose();
            }
         }
      }
   }

   private void WriteToSessionChannel(Result<INetworkSession, NetworkCodeError> result)
   {
      _sessionChannel.Writer.TryWrite(result);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      await UnbindAsync();

      await _ioQueueRegistry.DisposeAsync();
   }
}
