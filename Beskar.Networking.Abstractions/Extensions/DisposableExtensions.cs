
namespace Beskar.Networking.Abstractions.Extensions;

/// <summary>
/// Provides extension methods for disposing collections of disposable objects.
/// </summary>
public static class DisposableExtensions
{
   /// <param name="disposables">The collection of items to dispose.</param>
   /// <typeparam name="T">The type of items in the collection.</typeparam>
   extension<T>(IEnumerable<T>? disposables)
   {
      /// <summary>
      /// Asynchronously disposes all items in the collection that implement <see cref="IAsyncDisposable"/> or <see cref="IDisposable"/>.
      /// </summary>
      /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
      public async ValueTask DisposeAllAsync()
      {
         if (disposables is null)
         {
            return;
         }

         foreach (var item in disposables)
         {
            if (item is IAsyncDisposable asyncDisposable)
            {
               try
               {
                  await asyncDisposable.DisposeAsync();
               }
               catch
               {
                  // Ignored to ensure all elements are attempted to be disposed
               }
            }
            else if (item is IDisposable disposable)
            {
               try
               {
                  disposable.Dispose();
               }
               catch
               {
                  // Ignored to ensure all elements are attempted to be disposed
               }
            }
         }
      }
   }
}
