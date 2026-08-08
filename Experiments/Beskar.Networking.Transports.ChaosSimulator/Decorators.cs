using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Networking.Transports.ChaosSimulator;

public sealed class ChaosNetworkClient(INetworkClient inner, ChaosOptions options) : INetworkClient
{
   private readonly INetworkClient _inner = inner;
   private readonly ChaosOptions _options = options;

   private ChaosNetworkSession? _activeSession;

   public TransportKind Transport => _inner.Transport;
   public bool IsConnected => _activeSession is not null && _inner.IsConnected;
   public NetworkClientStats Stats => _inner.Stats;
   public INetworkSession? Session => _activeSession;
   public EndPoint? LocalAddress => _inner.LocalAddress;
   public EndPoint? RemoteAddress => _inner.RemoteAddress;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint,
      CancellationToken ct = default)
   {
      if (Random.Shared.NextDouble() < _options.ConnectFailureRate)
      {
         return new NetworkCodeError(-100, "Chaos: Connection attempt failed by design.");
      }

      if (_options.MaxConnectDelay > TimeSpan.Zero)
      {
         var delayMs = Random.Shared.Next((int)_options.MaxConnectDelay.TotalMilliseconds);
         if (delayMs > 0)
         {
            await Task.Delay(delayMs, ct);
         }
      }

      var result = await _inner.ConnectAsync(endPoint, ct);
      if (result.Failed)
      {
         return result.Error;
      }

      var session = new ChaosNetworkSession(result.Success, _options);
      _activeSession = session;
      return session;
   }

   public async ValueTask DisconnectAsync(CancellationToken ct = default)
   {
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         await session.DisposeAsync();
      }
      await _inner.DisconnectAsync(ct);
   }

   public async ValueTask DisposeAsync()
   {
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         await session.DisposeAsync();
      }
      await _inner.DisposeAsync();
   }
}

public sealed class ChaosNetworkListener(INetworkListener inner, ChaosOptions options) : INetworkListener
{
   private readonly INetworkListener _inner = inner;
   private readonly ChaosOptions _options = options;

   public EndPoint LocalAddress => _inner.LocalAddress;
   public bool IsBound => _inner.IsBound;
   public TransportKind Transport => _inner.Transport;
   public NetworkListenerStats Stats => _inner.Stats;

   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      return _inner.BindAsync(ct);
   }

   public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      return _inner.UnbindAsync(ct);
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(
      CancellationToken ct = default)
   {
      var result = await _inner.AcceptSessionAsync(ct);
      if (result.Failed)
      {
         return result.Error;
      }

      var session = new ChaosNetworkSession(result.Success, _options);
      return session;
   }

   public ValueTask DisposeAsync()
   {
      return _inner.DisposeAsync();
   }
}

public sealed class ChaosNetworkSession : INetworkSession
{
   private readonly INetworkSession _inner;
   private readonly ChaosOptions _options;
   private readonly CancellationTokenSource _sessionClosedCts = new();
   private readonly List<ChaosNetworkStream> _streams = [];
   private readonly Lock _lock = new();

   private int _disposed;

   public Guid Id => _inner.Id;
   public EndPoint RemoteAddress => _inner.RemoteAddress;
   public EndPoint LocalAddress => _inner.LocalAddress;
   public bool IsSupportingMultiplexing => _inner.IsSupportingMultiplexing;
   public bool IsSupportingUnidirectional => _inner.IsSupportingUnidirectional;
   public CancellationToken SessionClosedToken => _sessionClosedCts.Token;
   public INetworkPropertyStore Properties => _inner.Properties;
   public NetworkStats Stats => _inner.Stats;
   public DateTimeOffset CreatedAt => _inner.CreatedAt;
   public TransportKind Transport => _inner.Transport;
   public NetworkSecurityInfo SecurityInfo => _inner.SecurityInfo;
   public NetworkSessionStats SessionStats => _inner.SessionStats;

   public IReadOnlyCollection<INetworkStream> ActiveStreams
   {
      get
      {
         lock (_lock)
         {
            return _streams.ToArray();
         }
      }
   }

   public ChaosNetworkSession(INetworkSession inner, ChaosOptions options)
   {
      _inner = inner;
      _options = options;

      _inner.SessionClosedToken.Register(() => _sessionClosedCts.Cancel());

      if (_options.SessionAbruptDisconnectRate > 0 && Random.Shared.NextDouble() < _options.SessionAbruptDisconnectRate)
      {
         var minMs = (int)Math.Max(200, _options.SessionLifetimeMin.TotalMilliseconds);
         var maxMs = (int)Math.Max(800, _options.SessionLifetimeMax.TotalMilliseconds);
         if (maxMs < minMs) maxMs = minMs + 200;

         var lifetimeMs = Random.Shared.Next(minMs, maxMs);
         _sessionClosedCts.CancelAfter(lifetimeMs);
      }
   }

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(
      CancellationToken ct = default)
   {
      using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _sessionClosedCts.Token);

      if (Random.Shared.NextDouble() < _options.StreamOpenFailureRate)
      {
         return new NetworkCodeError(-200, "Chaos: Stream accept failed by design.");
      }

      if (_options.MaxStreamOpenDelay > TimeSpan.Zero)
      {
         var delayMs = Random.Shared.Next((int)_options.MaxStreamOpenDelay.TotalMilliseconds);
         if (delayMs > 0)
         {
            await Task.Delay(delayMs, linkedCts.Token);
         }
      }

      var result = await _inner.AcceptStreamAsync(linkedCts.Token);
      if (result.Failed)
      {
         return result.Error;
      }

      var stream = new ChaosNetworkStream(result.Success, this, _options);
      lock (_lock)
      {
         _streams.Add(stream);
      }
      return stream;
   }

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _sessionClosedCts.Token);

      // Simulate stream open failures/delays
      if (Random.Shared.NextDouble() < _options.StreamOpenFailureRate)
      {
         return new NetworkCodeError(-200, "Chaos: Stream open failed by design.");
      }

      if (_options.MaxStreamOpenDelay > TimeSpan.Zero)
      {
         var delayMs = Random.Shared.Next((int)_options.MaxStreamOpenDelay.TotalMilliseconds);
         if (delayMs > 0)
         {
            await Task.Delay(delayMs, linkedCts.Token);
         }
      }

      var result = await _inner.OpenStreamAsync(direction, linkedCts.Token);
      if (result.Failed)
      {
         return result.Error;
      }

      var stream = new ChaosNetworkStream(result.Success, this, _options);
      lock (_lock)
      {
         _streams.Add(stream);
      }
      return stream;
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      try
      {
         await _sessionClosedCts.CancelAsync();
      }
      catch
      {
         // Ignored
      }
      _sessionClosedCts.Dispose();

      List<ChaosNetworkStream> streamsToDispose;
      lock (_lock)
      {
         streamsToDispose = [.. _streams];
         _streams.Clear();
      }

      foreach (var stream in streamsToDispose)
      {
         await stream.DisposeAsync();
      }

      await _inner.DisposeAsync();
   }
}

public sealed class ChaosNetworkStream(INetworkStream inner, ChaosNetworkSession session, ChaosOptions options)
   : INetworkStream
{
   private readonly INetworkStream _inner = inner;
   private readonly ChaosNetworkSession _session = session;
   private readonly ChaosDuplexPipe _chaosPipe = new(inner.Transport, options);

   public long StreamId => _inner.StreamId;
   public INetworkSession Session => _session;
   public NetworkStreamDirection Direction => _inner.Direction;
   public IDuplexPipe Transport => _chaosPipe;
   public DateTimeOffset CreatedAt => _inner.CreatedAt;

   public NetworkStats Stats
   {
      get => _inner.Stats;
      set => _inner.Stats = value;
   }

   public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
   {
      return _inner.AcquireWriterLock(cancellationToken);
   }

   public async ValueTask DisposeAsync()
   {
      await _chaosPipe.DisposeAsync();
      await _inner.DisposeAsync();
   }
}
