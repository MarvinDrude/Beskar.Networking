using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Beskar.Memory.Owners;

/// <summary>
///    A high-performance, stack-only owner of a temporary <see cref="Span{T}" /> buffer rented from a memory pool.
///    Can also contain a <see cref="Span{T}" /> owned by the caller, which will do nothing if the owner is disposed.
/// </summary>
/// <typeparam name="T">The type of items stored in the buffer.</typeparam>
[StructLayout(LayoutKind.Auto)]
public ref struct SpanOwner<T>
{
   private ArrayPool<T>? _pool;
   private T[]? _buffer;
   private int _length;
   private Span<T> _span;

   /// <summary>
   ///    Gets a default, empty <see cref="SpanOwner{T}" />.
   /// </summary>
   public static SpanOwner<T> Empty
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get => default;
   }

   /// <summary>
   ///    Gets the active <see cref="Span{T}" /> representing the owned buffer.
   /// </summary>
   public Span<T> Span
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         if (_length < 0)
            ThrowObjectDisposedException();
         return _span;
      }
   }

   /// <summary>
   ///    Gets the active length of the owned buffer.
   /// </summary>
   public int Length
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         if (_length < 0)
            ThrowObjectDisposedException();
         return _length;
      }
   }

   /// <summary>
   ///    Accesses a single element within the owned buffer by index.
   /// </summary>
   public ref T this[int index]
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get => ref Span[index];
   }

   /// <summary>
   ///    Initializes a new instance of <see cref="SpanOwner{T}" /> wrapping an externally managed, non-pooled span.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public SpanOwner(Span<T> span)
   {
      _pool = null;
      _buffer = null;
      _length = span.Length;
      _span = span;
   }

   /// <summary>
   ///    Initializes a new instance of <see cref="SpanOwner{T}" /> renting a buffer of a specified minimum size.
   /// </summary>
   public SpanOwner(int minSize, bool clearArray = true, ArrayPool<T>? pool = null)
   {
      switch (minSize)
      {
         case < 0:
            throw new ArgumentOutOfRangeException(nameof(minSize), "Size must be non-negative.");
         case 0:
            _pool = null;
            _buffer = null;
            _span = [];
            _length = 0;
            return;
      }

      _pool = pool ?? ArrayPool<T>.Shared;
      _buffer = _pool.Rent(minSize);
      _span = _buffer.AsSpan(0, minSize);
      _length = minSize;

      if (clearArray)
         _span.Clear();
   }

   /// <summary>
   ///    Exposes the underlying pooled array and its active segment.
   /// </summary>
   public ArraySegment<T> DangerousGetArray()
   {
      if (_length < 0)
         ThrowObjectDisposedException();
      if (_buffer is null)
         throw new InvalidOperationException("This SpanOwner does not wrap a pooled array.");

      return new ArraySegment<T>(_buffer, 0, _length);
   }

   /// <summary>
   ///    Fills the owned buffer with a specified value.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Fill(T value)
   {
      Span.Fill(value);
   }

   /// <summary>
   ///    Clears the contents of the active buffer.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Clear()
   {
      Span.Clear();
   }

   /// <summary>
   ///    Copies the contents of this buffer to a destination span.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void CopyTo(Span<T> destination)
   {
      Span.CopyTo(destination);
   }

   /// <summary>
   ///    Attempts to copy the contents of this buffer to a destination span.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public bool TryCopyTo(Span<T> destination)
   {
      return Span.TryCopyTo(destination);
   }

   /// <summary>
   ///    Slices the owned buffer.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<T> Slice(int start, int length)
   {
      return Span.Slice(start, length);
   }

   /// <summary>
   ///    Get the span enumerator.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<T>.Enumerator GetEnumerator()
   {
      return Span.GetEnumerator();
   }

   /// <summary>
   ///    Transfers ownership of the underlying buffer to the caller.
   /// </summary>
   public T[]? Transfer(out int length)
   {
      if (_length < 0)
         ThrowObjectDisposedException();

      var buffer = _buffer;
      length = _length;

      _buffer = null;
      _pool = null;
      _span = default;
      _length = -1;

      return buffer;
   }

   /// <summary>
   ///    Returns the rented buffer to the pool and invalidates this owner.
   /// </summary>
   public void Dispose()
   {
      if (_length < 0) return;

      _length = -1;
      _span = [];

      var buffer = _buffer;
      if (buffer is null || _pool is null) return;

      _buffer = null;
      _pool.Return(buffer, !typeof(T).IsValueType);
   }

   /// <summary>
   ///    Rents a new buffer of the specified size.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static SpanOwner<T> Allocate(int size, bool clearArray = true, ArrayPool<T>? pool = null)
   {
      return new SpanOwner<T>(size, clearArray, pool);
   }

   /// <summary>
   ///    Allocates a new pooled buffer and copies the contents of the source span into it.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static SpanOwner<T> AllocateAndCopy(ReadOnlySpan<T> source, int minSize, bool clearArray = false,
      ArrayPool<T>? pool = null)
   {
      var owner = new SpanOwner<T>(minSize, clearArray, pool);
      source.CopyTo(owner.Span);
      return owner;
   }

   /// <summary>
   ///    Implicitly converts this <see cref="SpanOwner{T}" /> to a <see cref="Span{T}" />.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator Span<T>(SpanOwner<T> owner)
   {
      return owner.Span;
   }

   /// <summary>
   ///    Implicitly converts this <see cref="SpanOwner{T}" /> to a <see cref="ReadOnlySpan{T}" />.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator ReadOnlySpan<T>(SpanOwner<T> owner)
   {
      return owner.Span;
   }

   [MethodImpl(MethodImplOptions.NoInlining)]
   private static void ThrowObjectDisposedException()
   {
      throw new ObjectDisposedException(typeof(SpanOwner<T>).FullName);
   }
}
