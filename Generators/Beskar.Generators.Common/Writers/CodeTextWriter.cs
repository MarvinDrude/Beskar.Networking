using System.Runtime.CompilerServices;
using Beskar.Memory.Writers;

namespace Beskar.Memory.Code;

/// <summary>
///    A high-performance, stack-only ref struct writer designed specifically for code generation with automatic
///    indentation.
///    Wraps and optimizes contiguous memory operations of <see cref="TextWriterIndentSlim" />.
/// </summary>
public ref struct CodeTextWriter
{
   private const char DefaultNewLine = '\n';
   private const char DefaultIndent = ' ';
   private const int DefaultIndentSize = 3;

   /// <summary>
   ///    Gets a read-only span representing the already written code data.
   /// </summary>
   public readonly ReadOnlySpan<char> WrittenSpan
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get => _writer.WrittenSpan;
   }

   private TextWriterIndentSlim _writer;

   /// <summary>
   ///    Initializes a new instance of the <see cref="CodeTextWriter" /> struct.
   /// </summary>
   public CodeTextWriter(
      Span<char> buffer,
      Span<char> indentBuffer,
      int indentSize = DefaultIndentSize,
      char indentChar = DefaultIndent,
      char newLineChar = DefaultNewLine,
      int initialMinGrowCapacity = 1024)
   {
      _writer = new TextWriterIndentSlim(
         buffer,
         indentBuffer,
         indentSize,
         indentChar,
         newLineChar,
         initialMinGrowCapacity);
   }

   /// <summary>
   ///    Initializes a new instance of the <see cref="CodeTextWriter" /> struct with an automatically allocated indentation
   ///    cache buffer.
   /// </summary>
   public CodeTextWriter(
      Span<char> buffer,
      int indentSize = DefaultIndentSize,
      char indentChar = DefaultIndent,
      char newLineChar = DefaultNewLine,
      int initialMinGrowCapacity = 1024)
      : this(buffer, new char[128], indentSize, indentChar, newLineChar, initialMinGrowCapacity)
   {
   }

   /// <summary>
   ///    Writes an open curly bracket and increases the indentation level.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void OpenBody()
   {
      WriteLine("{");
      UpIndent();
   }

   /// <summary>
   ///    Decreases the indentation level and writes a close curly bracket.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void CloseBody()
   {
      DownIndent();
      WriteLine("}");
   }

   /// <summary>
   ///    Decreases the indentation level and writes a close curly bracket followed by a semicolon.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void CloseBodySemicolon()
   {
      DownIndent();
      WriteLine("};");
   }

   /// <summary>
   ///    Writes a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine()
   {
      _writer.WriteLine();
   }

   /// <summary>
   ///    Writes a new line character if the condition is met.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLineIf(bool condition)
   {
      _writer.WriteLineIf(condition);
   }

   /// <summary>
   ///    Writes the specified character span followed by a new line character if the condition is met.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLineIf(bool condition, ReadOnlySpan<char> content, bool multiLine = false)
   {
      _writer.WriteLineIf(condition, content, multiLine);
   }

   /// <summary>
   ///    Writes the specified character span followed by a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine(Span<char> content, bool multiLine = false)
   {
      _writer.WriteLine(content, multiLine);
   }

   /// <summary>
   ///    Writes the specified read-only character span followed by a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine(ReadOnlySpan<char> content, bool multiLine = false)
   {
      _writer.WriteLine(content, multiLine);
   }

   /// <summary>
   ///    Writes the specified string followed by a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine(string text, bool multiLine = false)
   {
      _writer.WriteLine(text, multiLine);
   }

   /// <summary>
   ///    Writes the specified string value.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteText(string text)
   {
      _writer.WriteText(text);
   }

   /// <summary>
   ///    Writes the specified read-only character span value.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteText(ReadOnlySpan<char> text)
   {
      _writer.WriteText(text);
   }

   /// <summary>
   ///    Writes the specified read-only character span value, optionally handling multiple lines.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(ReadOnlySpan<char> text, bool multiLine = false)
   {
      _writer.Write(text, multiLine);
   }

   /// <summary>
   ///    Writes the specified string value, optionally handling multiple lines.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(string text, bool multiLine = false)
   {
      _writer.Write(text, multiLine);
   }

   /// <summary>
   ///    Writes the specified read-only character span if the condition is met.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteIf(bool condition, ReadOnlySpan<char> content, bool multiLine = false)
   {
      _writer.WriteIf(condition, content, multiLine);
   }

   /// <summary>
   ///    Increases the current indentation level.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void UpIndent()
   {
      _writer.UpIndent();
   }

   /// <summary>
   ///    Decreases the current indentation level.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void DownIndent()
   {
      _writer.DownIndent();
   }

   /// <summary>
   ///    Acquires a span of the specified length.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<char> AcquireSpan(int length)
   {
      return _writer.AcquireSpan(length);
   }

   /// <summary>
   ///    Acquires a span of the specified length, automatically applying the current indentation level on demand first.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<char> AcquireSpanIndented(int length)
   {
      return _writer.AcquireSpanIndented(length);
   }

   /// <summary>
   ///    Gets a read-only character span representing the current indentation prefix.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public ReadOnlySpan<char> GetCurrentIndentBuffer()
   {
      return _writer.GetCurrentIndentBuffer();
   }

   /// <summary>
   ///    Returns a string representation of the written code content.
   /// </summary>
   public override string ToString()
   {
      return _writer.WrittenSpan.ToString();
   }

   /// <summary>
   ///    Disposes the writer, returning rented memory blocks.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Dispose()
   {
      _writer.Dispose();
   }
}
