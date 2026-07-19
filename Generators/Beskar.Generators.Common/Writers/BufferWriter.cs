using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Beskar.Memory.Owners;

namespace Beskar.Memory.Writers;

/// <summary>
///    A high-performance, stack-only <see langword="ref struct" /> writer for fast, contiguous memory operations.
///    Supports writing to an initial stack-allocated span and growing onto rented memory from an array pool if needed.
/// </summary>
/// <typeparam name="T">The type of items stored in the buffer.</typeparam>
[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerView, nq}")]
public ref struct BufferWriter<T> : IDisposable
{
   private SpanOwner<T> _initialSpanOwner;
   private MemoryOwner<T> _memoryOwner;
   private Span<T> _span;

   private bool _isGrown = false;
   private bool _isDisposed = false;

   private readonly int _initialMinGrowCapacity;
   private int _position = 0;

   /// <summary>
   ///    Gets the total capacity of the current active buffer.
   /// </summary>
   public readonly int Capacity
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         ThrowIfDisposed();
         return _span.Length;
      }
   }

   /// <summary>
   ///    Gets the remaining free space in the active buffer.
   /// </summary>
   public readonly int FreeCapacity
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         ThrowIfDisposed();
         return _span.Length - _position;
      }
   }

   /// <summary>
   ///    Gets a read-only span representing the already written data.
   /// </summary>
   public readonly ReadOnlySpan<T> WrittenSpan
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get
      {
         ThrowIfDisposed();
         return _span[.._position];
      }
   }

   /// <summary>
   ///    Gets or sets the current writing position within the active buffer.
   /// </summary>
   public int Position
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      readonly get
      {
         ThrowIfDisposed();
         return _position;
      }
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      set
      {
         ThrowIfDisposed();
         if (value > _span.Length || value < 0)
            throw new ArgumentOutOfRangeException(nameof(value),
               "Position must be non-negative and less than or equal to Capacity.");

         _position = value;
      }
   }

   /// <summary>
   ///    Initializes a new instance of the <see cref="BufferWriter{T}" /> struct with an initial stack-allocated or external
   ///    destination buffer.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public BufferWriter(
      Span<T> startBuffer,
      int initialMinGrowCapacity = -1)
   {
      _initialSpanOwner = new SpanOwner<T>(startBuffer);
      _memoryOwner = default;

      _initialMinGrowCapacity = initialMinGrowCapacity;
      _span = startBuffer;
   }

   /// <summary>
   ///    Initializes a new instance of the <see cref="BufferWriter{T}" /> struct pre-allocated and renting from the shared
   ///    array pool.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public BufferWriter(
      int minSize,
      int initialMinGrowCapacity = -1)
   {
      _memoryOwner = new MemoryOwner<T>(minSize);
      _initialSpanOwner = default;

      _isGrown = true;
      _initialMinGrowCapacity = initialMinGrowCapacity;
      _span = _memoryOwner.Span;
   }

   /// <summary>
   ///    Fills the entire active buffer with the specified value.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Fill(T value)
   {
      ThrowIfDisposed();
      _span.Fill(value);
   }

   /// <summary>
   ///    Appends a single item by reference to the writer, growing the buffer if necessary.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Add(ref T reference)
   {
      ThrowIfDisposed();
      if (_span.Length - _position < 1) Resize(1);

      ref var refr = ref MemoryMarshal.GetReference(_span);
      Unsafe.Add(ref refr, _position++) = reference;
   }

   /// <summary>
   ///    Appends a single item by value to the writer, growing the buffer if necessary.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Add(T value)
   {
      ThrowIfDisposed();
      if (_span.Length - _position < 1) Resize(1);

      ref var reference = ref MemoryMarshal.GetReference(_span);
      Unsafe.Add(ref reference, _position++) = value;
   }

   /// <summary>
   ///    Writes a span of items to the writer, growing the buffer if necessary.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(ReadOnlySpan<T> span)
   {
      ThrowIfDisposed();
      if (span.IsEmpty) return;

      var freeCapacity = _span.Length - _position;
      if (freeCapacity < span.Length) Resize(span.Length - freeCapacity);

      if (!typeof(T).IsValueType)
      {
         span.CopyTo(_span[_position..]);
      }
      else
      {
         ref var srcBase = ref Unsafe.As<T, byte>(ref MemoryMarshal.GetReference(span));
         ref var destBase = ref Unsafe.As<T, byte>(ref MemoryMarshal.GetReference(_span));

         var sizeOf = Unsafe.SizeOf<T>();
         var byteCount = (uint)(span.Length * sizeOf);

         Unsafe.CopyBlockUnaligned(
            ref Unsafe.AddByteOffset(ref destBase, (IntPtr)(_position * sizeOf)),
            ref srcBase,
            byteCount);
      }

      _position += span.Length;
   }

   /// <summary>
   ///    Acquires a slice of the free buffer of the specified length, growing the buffer if necessary.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<T> AcquireSpan(int length, bool movePosition = true)
   {
      ThrowIfDisposed();
      var start = _position;

      var freeCapacity = _span.Length - _position;
      if (freeCapacity < length) Resize(length - freeCapacity);

      if (movePosition) _position += length;

      return _span.Slice(start, length);
   }

   /// <summary>
   ///    Advances the writing position by the specified count, growing the buffer if necessary.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Advance(int count)
   {
      ThrowIfDisposed();
      var freeCapacity = _span.Length - _position;
      if (freeCapacity < count) Resize(count - freeCapacity);

      _position += count;
   }

   /// <summary>
   ///    Advances the writing position directly to the specified position, growing the buffer if necessary.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void AdvanceTo(int position)
   {
      ThrowIfDisposed();
      if (_span.Length < position) Resize(position - _span.Length);

      _position = position;
   }

   /// <summary>
   ///    Shifts a segment of the active buffer to a new position, growing the buffer if necessary.
   /// </summary>
   public void Move(
      int fromStart, int fromSize,
      int toStart, bool movePosition = true)
   {
      ThrowIfDisposed();
      if (fromSize == 0 || fromStart == toStart)
      {
         if (toStart > _span.Length) Resize(toStart - _span.Length);

         return;
      }

      var oldPosition = _position;
      var newPosition = toStart + fromSize;

      if (newPosition > _span.Length) Resize(newPosition - _span.Length);

      _span.Slice(fromStart, fromSize)
         .CopyTo(_span.Slice(toStart, fromSize));
      _position = movePosition ? newPosition : oldPosition;
   }

   /// <summary>
   ///    Gets the underlying <see cref="MemoryOwner{T}" /> representing the grown buffer.
   /// </summary>
   public readonly MemoryOwner<T> GetMemoryOwner()
   {
      ThrowIfDisposed();
      if (!_isGrown) throw new InvalidOperationException("BufferWriter is not grown");

      return _memoryOwner;
   }

   internal void Resize(int requestedSize)
   {
      int newSize;
      var currentLength = _isGrown ? _memoryOwner.Length : _initialSpanOwner.Length;

      if (!_isGrown && _initialMinGrowCapacity >= requestedSize)
      {
         newSize = currentLength + _initialMinGrowCapacity;
      }
      else
      {
         var growBy = Math.Max(requestedSize, Math.Max(currentLength, 256));
         var newSizeLong = (long)currentLength + growBy;
         newSize = (int)Math.Min(newSizeLong, (long)int.MaxValue - 1);
      }

      var newOwner = new MemoryOwner<T>(newSize);
      _span.Slice(0, _position).CopyTo(newOwner.Span);

      if (_isGrown)
      {
         _memoryOwner.Dispose();
      }
      else
      {
         _initialSpanOwner.Dispose();
         _isGrown = true;
      }

      _memoryOwner = newOwner;
      _span = _memoryOwner.Span;
   }

   /// <summary>
   ///    Disposes the writer, returning any rented buffers to the pool and clearing internal references.
   /// </summary>
   public void Dispose()
   {
      if (_isDisposed) return;

      _isDisposed = true;

      if (_isGrown)
         _memoryOwner.Dispose();
      else
         _initialSpanOwner.Dispose();

      _span = [];
      _position = -1;
   }

   /// <summary>
   ///    Implicitly converts this <see cref="BufferWriter{T}" /> to a <see cref="ReadOnlySpan{T}" /> of the written data.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator ReadOnlySpan<T>(BufferWriter<T> writer)
   {
      return writer.WrittenSpan;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private readonly void ThrowIfDisposed()
   {
      if (_isDisposed) throw new ObjectDisposedException(typeof(BufferWriter<T>).FullName);
   }

   private readonly string DebuggerView => $"BufferWriter(Grown={_isGrown})[Pos={_position}/Cap={_span.Length}]";
}
