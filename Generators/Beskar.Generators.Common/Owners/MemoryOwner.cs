using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Beskar.Memory.Owners;

/// <summary>
///    A high-performance, disposable owner of a temporary <see cref="Memory{T}" /> buffer rented from a memory pool.
///    Can also wrap an externally managed, non-pooled buffer.
/// </summary>
/// <typeparam name="T">The type of items stored in the buffer.</typeparam>
[StructLayout(LayoutKind.Auto)]
public struct MemoryOwner<T> : IDisposable
{
   private ArrayPool<T>? _pool;
   private T[]? _buffer;
   private int _length;

   /// <summary>
   ///    Gets a default, empty <see cref="MemoryOwner{T}" />.
   /// </summary>
   public static MemoryOwner<T> Empty
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get => default;
   }

   /// <summary>
   ///    Gets the capacity of the underlying rented buffer.
   /// </summary>
   public int Capacity
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         if (_length < 0)
            ThrowObjectDisposedException();
         return _buffer?.Length ?? 0;
      }
   }

   /// <summary>
   ///    Gets or sets the active length of the owned buffer.
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
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      set
      {
         if (_length < 0)
            ThrowObjectDisposedException();
         if (value < 0 || (_buffer is not null && value > _buffer.Length))
            throw new ArgumentOutOfRangeException(nameof(value),
               "Length must be non-negative and less than or equal to Capacity.");

         _length = value;
      }
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
         return _buffer is null ? [] : _buffer.AsSpan(0, _length);
      }
   }

   /// <summary>
   ///    Gets the active <see cref="Memory{T}" /> representing the owned buffer.
   /// </summary>
   public Memory<T> Memory
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         if (_length < 0)
            ThrowObjectDisposedException();
         return _buffer?.AsMemory(0, _length) ?? default;
      }
   }

   /// <summary>
   ///    Gets the underlying array buffer.
   /// </summary>
   public T[] Buffer
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         if (_length < 0)
            ThrowObjectDisposedException();
         return _buffer ?? [];
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
   ///    Initializes a new instance of <see cref="MemoryOwner{T}" /> wrapping an externally managed, non-pooled array.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public MemoryOwner(T[] array)
   {
      _buffer = array;
      _length = array.Length;
      _pool = null;
   }

   /// <summary>
   ///    Initializes a new instance of <see cref="MemoryOwner{T}" /> renting a buffer of a specified minimum size.
   /// </summary>
   public MemoryOwner(int minSize, bool clearArray = true, ArrayPool<T>? pool = null)
   {
      switch (minSize)
      {
         case < 0:
            throw new ArgumentOutOfRangeException(nameof(minSize), "Size must be non-negative.");
         case 0:
            _buffer = null;
            _pool = null;
            _length = 0;
            return;
      }

      _pool = pool ?? ArrayPool<T>.Shared;
      _buffer = _pool.Rent(minSize);
      _length = minSize;

      if (clearArray) _buffer.AsSpan(0, minSize).Clear();
   }

   /// <summary>
   ///    Attempts to resize the active length of the owned buffer.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public bool TryResize(int newSize)
   {
      if (_length < 0)
         ThrowObjectDisposedException();
      if (newSize < 0)
         throw new ArgumentOutOfRangeException(nameof(newSize), "Size must be non-negative.");
      if (newSize > Capacity)
         return false;

      _length = newSize;
      return true;
   }

   /// <summary>
   ///    Exposes the underlying pooled array and its active segment.
   /// </summary>
   public ArraySegment<T> DangerousGetArray()
   {
      if (_length < 0)
         ThrowObjectDisposedException();
      if (_buffer is null)
         throw new InvalidOperationException("This MemoryOwner does not wrap a pooled array.");

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
   ///    Copies the contents of this buffer to a destination memory.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void CopyTo(Memory<T> destination)
   {
      Memory.CopyTo(destination);
   }

   /// <summary>
   ///    Attempts to copy the contents of this buffer to a destination memory.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public bool TryCopyTo(Memory<T> destination)
   {
      return Memory.TryCopyTo(destination);
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

      var buffer = _buffer;
      if (buffer is null || _pool is null) return;

      _buffer = null;
      _pool.Return(buffer, !typeof(T).IsValueType);
   }

   /// <summary>
   ///    Rents a new buffer of the specified size.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static MemoryOwner<T> Allocate(int size, bool clearArray = true, ArrayPool<T>? pool = null)
   {
      return new MemoryOwner<T>(size, clearArray, pool);
   }

   /// <summary>
   ///    Allocates a new pooled buffer and copies the contents of the source span into it.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static MemoryOwner<T> AllocateAndCopy(ReadOnlySpan<T> source, int minSize, bool clearArray = false,
      ArrayPool<T>? pool = null)
   {
      var owner = new MemoryOwner<T>(minSize, clearArray, pool);
      source.CopyTo(owner.Span);
      return owner;
   }

   /// <summary>
   ///    Implicitly converts this <see cref="MemoryOwner{T}" /> to a <see cref="Memory{T}" />.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator Memory<T>(MemoryOwner<T> owner)
   {
      return owner.Memory;
   }

   /// <summary>
   ///    Implicitly converts this <see cref="MemoryOwner{T}" /> to a <see cref="ReadOnlyMemory{T}" />.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator ReadOnlyMemory<T>(MemoryOwner<T> owner)
   {
      return owner.Memory;
   }

   /// <summary>
   ///    Implicitly converts this <see cref="MemoryOwner{T}" /> to a <see cref="Span{T}" />.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator Span<T>(MemoryOwner<T> owner)
   {
      return owner.Span;
   }

   /// <summary>
   ///    Implicitly converts this <see cref="MemoryOwner{T}" /> to a <see cref="ReadOnlySpan{T}" />.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator ReadOnlySpan<T>(MemoryOwner<T> owner)
   {
      return owner.Span;
   }

   [MethodImpl(MethodImplOptions.NoInlining)]
   private static void ThrowObjectDisposedException()
   {
      throw new ObjectDisposedException(typeof(MemoryOwner<T>).FullName);
   }
}
