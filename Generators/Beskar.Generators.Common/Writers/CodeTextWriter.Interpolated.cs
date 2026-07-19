#pragma warning disable CS8500
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Beskar.Memory.Code;

/// <summary>
///    Provides extension methods for writing interpolated strings to a <see cref="CodeTextWriter" />.
/// </summary>
public static class CodeTextWriterInterpolatedStringHandlerExtensions
{
   /// <summary>
   ///    Writes an interpolated string with a format provider.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static void WriteInterpolated(this ref CodeTextWriter writer, IFormatProvider? provider,
      [InterpolatedStringHandlerArgument(nameof(writer), nameof(provider))]
      scoped ref CodeTextWriterInterpolatedStringHandler handler)
   {
   }

   /// <summary>
   ///    Writes an interpolated string with a format provider followed by a new line.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static void WriteLineInterpolated(this ref CodeTextWriter writer, IFormatProvider? provider,
      [InterpolatedStringHandlerArgument(nameof(writer), nameof(provider))]
      scoped ref CodeTextWriterInterpolatedStringHandler handler)
   {
      writer.WriteLine();
   }

   /// <summary>
   ///    Writes an interpolated string.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static void WriteInterpolated(this ref CodeTextWriter writer,
      [InterpolatedStringHandlerArgument(nameof(writer))]
      scoped ref CodeTextWriterInterpolatedStringHandler handler)
   {
   }

   /// <summary>
   ///    Writes an interpolated string followed by a new line.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static void WriteLineInterpolated(this ref CodeTextWriter writer,
      [InterpolatedStringHandlerArgument(nameof(writer))]
      scoped ref CodeTextWriterInterpolatedStringHandler handler)
   {
      writer.WriteLine();
   }
}

/// <summary>
///    A high-performance, non-allocating ref struct interpolated string handler for <see cref="CodeTextWriter" />.
/// </summary>
[InterpolatedStringHandler]
[EditorBrowsable(EditorBrowsableState.Never)]
[StructLayout(LayoutKind.Auto)]
public unsafe ref struct CodeTextWriterInterpolatedStringHandler
{
   private readonly nint _writerPointer;

   private ref CodeTextWriter Writer
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get => ref *(CodeTextWriter*)_writerPointer;
   }

   /// <summary>
   ///    Gets the number of characters written.
   /// </summary>
   public int Count { get; private set; }

   private readonly IFormatProvider? _provider;

   /// <summary>
   ///    Initializes a new instance of the <see cref="CodeTextWriterInterpolatedStringHandler" /> struct.
   /// </summary>
   public CodeTextWriterInterpolatedStringHandler(
      int literalLength,
      int formattedCount,
      scoped ref CodeTextWriter writer,
      IFormatProvider? provider = null)
   {
      fixed (CodeTextWriter* p = &writer)
      {
         _writerPointer = (nint)p;
      }

      _provider = provider;
      Count = 0;
   }

   /// <summary>
   ///    Appends a literal string value.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void AppendLiteral(string? value)
   {
      if (value is null) return;
      AppendFormatted(value.AsSpan());
   }

   /// <summary>
   ///    Appends a formatted string value.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void AppendFormatted(string value)
   {
      AppendFormatted(value.AsSpan());
   }

   /// <summary>
   ///    Appends a formatted span of characters.
   /// </summary>
   public void AppendFormatted(ReadOnlySpan<char> value)
   {
      Writer.Write(value);
      Count += value.Length;
   }

   /// <summary>
   ///    Appends a formatted generic value.
   /// </summary>
   public void AppendFormatted<T>(T value, string? format = null)
   {
      Count += AppendFormattedInternal(value, format);
   }

   private int AppendFormattedInternal<T>(T value, string? format)
   {
#if NET6_0_OR_GREATER
      if (value is ISpanFormattable spanFormattable)
      {
         Span<char> buffer = stackalloc char[256];
         if (spanFormattable.TryFormat(buffer, out var charsWritten, format, _provider))
         {
            Writer.Write(buffer[..charsWritten]);
            return charsWritten;
         }
      }
#endif

      var charsWrittenFallback = value switch
      {
         IFormattable formattable => Write(ref Writer, formattable.ToString(format, _provider)),
         not null => Write(ref Writer, value.ToString()),
         _ => 0
      };

      return charsWrittenFallback;

      static int Write(ref CodeTextWriter writer, ReadOnlySpan<char> chars)
      {
         writer.Write(chars);
         return chars.Length;
      }
   }

   /// <summary>
   ///    Returns a string representation of the written buffer content.
   /// </summary>
   public override string ToString()
   {
      return Writer.WrittenSpan.ToString();
   }
}
