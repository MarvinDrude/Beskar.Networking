using System.Net;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Common.Hosting;

public sealed class NetworkServer
{
   private readonly List<EndpointDefinition> _definitions;
   private CancellationTokenSource? _cts;
   private List<Task>? _listenTasks;

   internal NetworkServer(List<EndpointDefinition> definitions)
   {
      _definitions = definitions;
   }

   public async Task StartAsync(CancellationToken ct = default)
   {
      _cts = new CancellationTokenSource();
      var bindTasks = _definitions.Select(async d =>
      {
         var result = await d.Listener.BindAsync(_cts.Token);
         if (!result.IsSuccess)
         {
            throw new InvalidOperationException($"Failed to bind listener on {d.EndPoint}: {result.Error.Message}");
         }
      });

      await Task.WhenAll(bindTasks);
      _listenTasks = [.. _definitions.Select(d => Task.Run(() => AcceptLoopAsync(d, _cts.Token), ct))];
   }

   private async Task AcceptLoopAsync(EndpointDefinition definition, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         try
         {
            var result = await definition.Listener.AcceptSessionAsync(token);
            if (result.Failed)
            {
               continue;
            }

            var session = result.Success;
            _ = Task.Run(async () =>
            {
               try
               {
                  var streamResult = await session.AcceptStreamAsync(token);
                  if (!streamResult.Failed)
                  {
                     var stream = streamResult.Success;
                     await using (stream)
                     {
                        await definition.Pipeline(stream.Transport, async () =>
                        {
                           await definition.SessionHandler(session);
                        });
                     }
                  }
               }
               catch
               {
                  // ignored
               }
               finally
               {
                  if (session is IAsyncDisposable asyncDisposable)
                  {
                     await asyncDisposable.DisposeAsync();
                  }
               }
            }, token);
         }
         catch (OperationCanceledException) when (token.IsCancellationRequested)
         {
            break;
         }
         catch
         {
            // ignored
         }
      }
   }

   public async Task StopAsync()
   {
      if (_cts is not null)
      {
         await _cts.CancelAsync();
      }

      var unbindTasks = _definitions.Select(d => d.Listener.UnbindAsync().AsTask());
      await Task.WhenAll(unbindTasks);

      if (_listenTasks is not null)
      {
         await Task.WhenAll(_listenTasks);
      }

      _cts?.Dispose();
   }
}
