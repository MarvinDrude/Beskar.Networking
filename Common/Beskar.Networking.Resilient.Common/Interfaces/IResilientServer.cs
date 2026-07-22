using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Resilient.Common.Enums;

namespace Beskar.Networking.Resilient.Common.Interfaces;

/// <summary>
/// Represents a resilient server capable of handling network connections.
/// </summary>
public interface IResilientServer : IAsyncDisposable
{
   /// <summary>
   /// Gets the current state of the resilient server during its lifecycle.
   /// </summary>
   /// <remarks>
   /// The state is represented using the <c>ResilientServerState</c> enumeration.
   /// It indicates whether the server is starting, running, stopping, or stopped.
   /// </remarks>
   public ResilientServerState State { get; }

   /// <summary>
   /// Indicates whether the resilient server is currently in a running state.
   /// </summary>
   /// <remarks>
   /// Returns <c>true</c> if the server's state is <c>ResilientServerState.Running</c>;
   /// otherwise, returns <c>false</c>.
   /// </remarks>
   public bool IsRunning { get; }

   /// <summary>
   /// Gets the collection of network listeners managed by the resilient server.
   /// </summary>
   /// <remarks>
   /// Each listener in the collection implements the <c>INetworkListener</c> interface
   /// and is responsible for handling incoming connections for a specific transport protocol
   /// (e.g., TCP, UDP, or WebSocket). These listeners are used to manage and monitor the
   /// network communication handled by the server.
   /// </remarks>
   public IReadOnlyList<INetworkListener> Listeners { get; }

   /// <summary>
   /// Asynchronously starts the resilient server and transitions it to the Running state.
   /// </summary>
   /// <returns>
   /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
   /// The result encapsulates a <see cref="VoidResult{StringError}"/> indicating success or an error if the server fails to start.
   /// </returns>
   public Task<VoidResult<StringError>> StartAsync();

   /// <summary>
   /// Asynchronously stops the resilient server and transitions it to the Stopped state.
   /// </summary>
   /// <returns>
   /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
   /// The result encapsulates a <see cref="VoidResult{StringError}"/> indicating success or an error if the server fails to stop.
   /// </returns>
   public Task<VoidResult<StringError>> StopAsync();
}
