using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Beskar.Generators.Common;

/// <summary>
/// A high-performance, immutable-by-contract, readonly struct wrapping a contiguous array.
/// Exposes sequence-based equality, span operations, and list interfaces.
/// </summary>
[CollectionBuilder(typeof(SequenceArrayCollectionBuilder), nameof(SequenceArrayCollectionBuilder.Create))]
public readonly struct SequenceArray<T>
   : IReadOnlyList<T>,
     IEquatable<SequenceArray<T>>
     where T : IEquatable<T>
{
   /// <summary>
   /// Gets the underlying array. Returns an empty array if constructed via default/uninitialized state.
   /// </summary>
   public T[] Array => field ?? [];

   /// <summary>
   /// Gets a mutable span over the underlying array.
   /// </summary>
   public Span<T> Span => Array.AsSpan();

   /// <summary>
   /// Gets a mutable memory region over the underlying array.
   /// </summary>
   public Memory<T> Memory => Array.AsMemory();

   /// <summary>
   /// Gets the number of elements in the array.
   /// </summary>
   public int Length => Array.Length;

   /// <summary>
   /// Gets the number of elements in the array.
   /// </summary>
   public int Count => Array.Length;

   /// <summary>
   /// Gets a reference to the element at the specified index.
   /// </summary>
   public ref readonly T this[int index] => ref Array[index];

   /// <summary>
   /// Gets a reference to the element at the specified index.
   /// </summary>
   public ref readonly T this[Index index] => ref Array[index];

   /// <summary>
   /// Gets a slice of the array.
   /// </summary>
   public SequenceArray<T> this[Range range]
   {
      get
      {
         var (offset, length) = range.GetOffsetAndLength(Array.Length);
         return new(Array.AsSpan(offset, length).ToArray());
      }
   }

   /// <summary>
   /// Initializes a new instance of the <see cref="SequenceArray{T}"/> struct wrapping the specified array.
   /// </summary>
   public SequenceArray(T[] array)
   {
      Array = array;
   }

   /// <summary>
   /// Initializes a new instance of the <see cref="SequenceArray{T}"/> struct copying from the specified span.
   /// </summary>
   public SequenceArray(ReadOnlySpan<T> span)
   {
      Array = span.ToArray();
   }

   /// <summary>
   /// Gets an allocation-free enumerator over the span.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<T>.Enumerator GetEnumerator()
   {
      return Span.GetEnumerator();
   }

   /// <summary>
   /// Determines whether this sequence array is equal to another.
   /// </summary>
   public bool Equals(SequenceArray<T> other)
   {
      if (ReferenceEquals(Array, other.Array))
      {
         return true;
      }
      return Span.SequenceEqual(other.Span);
   }

   /// <summary>
   /// Determines whether this sequence array is equal to an object.
   /// </summary>
   public override bool Equals([NotNullWhen(true)] object? obj)
   {
      return obj is SequenceArray<T> other && Equals(other);
   }

   /// <summary>
   /// Computes the hash code of the sequence.
   /// </summary>
   public override int GetHashCode()
   {
      var hash = new HashCode();

      foreach (ref var item in Span)
      {
         hash.Add(item);
      }

      return hash.ToHashCode();
   }

   T IReadOnlyList<T>.this[int index] => Array[index];

   IEnumerator<T> IEnumerable<T>.GetEnumerator()
   {
      return ((IEnumerable<T>)Array).GetEnumerator();
   }

   IEnumerator IEnumerable.GetEnumerator()
   {
      return Array.GetEnumerator();
   }

   /// <summary>
   /// Implicitly converts a <see cref="SequenceArray{T}"/> to a <see cref="ReadOnlySpan{T}"/>.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator ReadOnlySpan<T>(SequenceArray<T> array) => array.Span;

   /// <summary>
   /// Implicitly converts a <see cref="SequenceArray{T}"/> to a <see cref="Span{T}"/>.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator Span<T>(SequenceArray<T> array) => array.Span;

   /// <summary>
   /// Implicitly converts a <see cref="SequenceArray{T}"/> to a <see cref="Memory{T}"/>.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator Memory<T>(SequenceArray<T> array) => array.Memory;

   /// <summary>
   /// Implicitly converts a <see cref="SequenceArray{T}"/> to a <see cref="ReadOnlyMemory{T}"/>.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator ReadOnlyMemory<T>(SequenceArray<T> array) => array.Memory;

   /// <summary>
   /// Implicitly converts a raw array to a <see cref="SequenceArray{T}"/>.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static implicit operator SequenceArray<T>(T[] array) => new(array);

   /// <summary>
   /// Determines whether two sequence arrays are equal.
   /// </summary>
   public static bool operator ==(SequenceArray<T> left, SequenceArray<T> right)
   {
      return left.Equals(right);
   }

   /// <summary>
   /// Determines whether two sequence arrays are not equal.
   /// </summary>
   public static bool operator !=(SequenceArray<T> left, SequenceArray<T> right)
   {
      return !left.Equals(right);
   }
}

/// <summary>
/// Provides collection builder functionality for <see cref="SequenceArray{T}"/>.
/// </summary>
public static class SequenceArrayCollectionBuilder
{
   /// <summary>
   /// Creates a new <see cref="SequenceArray{T}"/> from the specified values span.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static SequenceArray<T> Create<T>(ReadOnlySpan<T> values) where T : IEquatable<T> => new(values);
}
