namespace Beskar.Jobs.Common.Interfaces;

/// <summary>
/// Defines a handler that executes a job of type <typeparamref name="TArgs"/> without returning a result payload.
/// </summary>
public interface IJobHandler<in TArgs>
{
   /// <summary>
   /// Executes the job asynchronously.
   /// </summary>
   public ValueTask ExecuteAsync(IJobExecutionContext<TArgs> context, CancellationToken ct = default);
}

/// <summary>
/// Defines a handler that executes a job of type <typeparamref name="TArgs"/> and produces a typed <typeparamref name="TResult"/>.
/// </summary>
public interface IJobHandlerWithResult<in TArgs, TResult>
{
   /// <summary>
   /// Executes the job asynchronously and returns a result that is serialized and broadcast upon completion.
   /// </summary>
   public ValueTask<TResult> ExecuteAsync(IJobExecutionContext<TArgs> context, CancellationToken ct = default);
}
