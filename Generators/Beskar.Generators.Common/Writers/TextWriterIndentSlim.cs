using System.Runtime.CompilerServices;

namespace Beskar.Memory.Writers;

/// <summary>
///    A high-performance, stack-only ref struct writer for formatted text with automatic indentation.
///    Supports writing to an initial stack-allocated span and growing onto rented memory from an array pool if needed.
/// </summary>
public ref struct TextWriterIndentSlim
{
   private const char DefaultNewLine = '\n';
   private const char DefaultIndent = ' ';
   private const int DefaultIndentSize = 3;

   /// <summary>
   ///    Gets a read-only span representing the already written data.
   /// </summary>
   public readonly ReadOnlySpan<char> WrittenSpan
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get => _buffer.WrittenSpan;
   }

   /// <summary>
   ///    Gets the character used for indentation.
   /// </summary>
   public char IndentCharacter
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get;
   }

   /// <summary>
   ///    Gets the character used for a new line.
   /// </summary>
   public char NewLineCharacter
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get;
   }

   /// <summary>
   ///    Gets the size (number of characters) for a single indentation level.
   /// </summary>
   public int IndentSize
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get;
   }

   /// <summary>
   ///    Gets the current indentation level.
   /// </summary>
   public int CurrentIndentLevel
   {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      get;
      private set;
   }

   private BufferWriter<char> _indentCache;
   private ReadOnlySpan<char> _currentLevelBuffer;

   private BufferWriter<char> _buffer;

   /// <summary>
   ///    Initializes a new instance of the <see cref="TextWriterIndentSlim" /> struct.
   /// </summary>
   public TextWriterIndentSlim(
      Span<char> buffer,
      Span<char> indentBuffer,
      int indentSize = DefaultIndentSize,
      char indentChar = DefaultIndent,
      char newLineChar = DefaultNewLine,
      int initialMinGrowCapacity = 1024)
   {
      NewLineCharacter = newLineChar;
      IndentCharacter = indentChar;
      IndentSize = indentSize;

      _buffer = new BufferWriter<char>(buffer, initialMinGrowCapacity);
      CurrentIndentLevel = 0;

      _indentCache = new BufferWriter<char>(indentBuffer);
      _indentCache.Fill(IndentCharacter);
      _currentLevelBuffer = [];
   }

   /// <summary>
   ///    Initializes a new instance of the <see cref="TextWriterIndentSlim" /> struct with an automatically allocated
   ///    indentation cache buffer.
   /// </summary>
   public TextWriterIndentSlim(
      Span<char> buffer,
      int indentSize = DefaultIndentSize,
      char indentChar = DefaultIndent,
      char newLineChar = DefaultNewLine,
      int initialMinGrowCapacity = 1024)
      : this(buffer, new char[128], indentSize, indentChar, newLineChar, initialMinGrowCapacity)
   {
   }

   /// <summary>
   ///    Writes a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine()
   {
      _buffer.Add(NewLineCharacter);
   }

   /// <summary>
   ///    Writes a new line character if the condition is met.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLineIf(bool condition)
   {
      if (condition) WriteLine();
   }

   /// <summary>
   ///    Writes the specified character span followed by a new line character if the condition is met.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLineIf(bool condition, ReadOnlySpan<char> content, bool multiLine = false)
   {
      if (condition) WriteLine(content, multiLine);
   }

   /// <summary>
   ///    Writes the specified character span followed by a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine(Span<char> content, bool multiLine = false)
   {
      Write(content, multiLine);
      WriteLine();
   }

   /// <summary>
   ///    Writes the specified read-only character span followed by a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine(ReadOnlySpan<char> content, bool multiLine = false)
   {
      Write(content, multiLine);
      WriteLine();
   }

   /// <summary>
   ///    Writes the specified string followed by a new line character.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteLine(string text, bool multiLine = false)
   {
      Write(text.AsSpan(), multiLine);
      WriteLine();
   }

   /// <summary>
   ///    Writes a string text.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteText(string text)
   {
      WriteText(text.AsSpan());
   }

   /// <summary>
   ///    Writes a read-only character span text.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteText(ReadOnlySpan<char> text)
   {
      AddIndentOnDemand();
      _buffer.Write(text);
   }

   /// <summary>
   ///    Writes a read-only span of characters, optionally handling multiple lines.
   /// </summary>
   public void Write(ReadOnlySpan<char> text, bool multiLine = false)
   {
      if (!multiLine)
         WriteText(text);
      else
         while (text.Length > 0)
         {
            var newLinePos = text.IndexOf(NewLineCharacter);

            if (newLinePos >= 0)
            {
               var line = text.Slice(0, newLinePos);

               WriteIf(!line.IsEmpty, line);
               WriteLine();

               text = text.Slice(newLinePos + 1);
            }
            else
            {
               WriteText(text);
               break;
            }
         }
   }

   /// <summary>
   ///    Writes a string of characters, optionally handling multiple lines.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(string text, bool multiLine = false)
   {
      Write(text.AsSpan(), multiLine);
   }

   /// <summary>
   ///    Writes a read-only character span if the condition is met.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void WriteIf(bool condition, ReadOnlySpan<char> content, bool multiLine = false)
   {
      if (condition) Write(content, multiLine);
   }

   /// <summary>
   ///    Increases the current indentation level.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void UpIndent()
   {
      CurrentIndentLevel++;
      _currentLevelBuffer = GetCurrentIndentBuffer();
   }

   /// <summary>
   ///    Decreases the current indentation level.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void DownIndent()
   {
      CurrentIndentLevel--;
      if (CurrentIndentLevel < 0)
         throw new ArgumentOutOfRangeException(nameof(CurrentIndentLevel), "Indentation level cannot be negative.");

      _currentLevelBuffer = GetCurrentIndentBuffer();
   }

   /// <summary>
   ///    Acquires a span of the specified length.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<char> AcquireSpan(int length)
   {
      return _buffer.AcquireSpan(length);
   }

   /// <summary>
   ///    Acquires a span of the specified length, automatically applying the current indentation level on demand first.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public Span<char> AcquireSpanIndented(int length)
   {
      AddIndentOnDemand();
      return _buffer.AcquireSpan(length);
   }

   private void AddIndentOnDemand()
   {
      if (_currentLevelBuffer.IsEmpty) return;

      if (_buffer.Position == 0 || _buffer.WrittenSpan[_buffer.WrittenSpan.Length - 1] == NewLineCharacter)
         _buffer.Write(_currentLevelBuffer);
   }

   /// <summary>
   ///    Gets a read-only character span representing the current indentation prefix.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public ReadOnlySpan<char> GetCurrentIndentBuffer()
   {
      if (CurrentIndentLevel == 0) return [];

      var levelCount = IndentSize * CurrentIndentLevel;

      while (_indentCache.Position < levelCount) _indentCache.Add(IndentCharacter);

      return _indentCache.WrittenSpan.Slice(0, levelCount);
   }

   /// <summary>
   ///    Returns a string representation of the written text content.
   /// </summary>
   public override string ToString()
   {
      return _buffer.WrittenSpan.ToString();
   }

   /// <summary>
   ///    Disposes the writer, returning rented memory blocks.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Dispose()
   {
      _buffer.Dispose();
      _indentCache.Dispose();
   }
}
