namespace Beskar.Utilities.Console.Rendering;

public static partial class ConsoleRender
{
    /// <summary>
    /// Creates a new fluent Console Table instance.
    /// </summary>
    public static ConsoleTable CreateTable() => new();
}

public class ConsoleTable
{
    public class Column(string header, Alignment alignment = Alignment.Left, ConsoleColor? color = null)
    {
        public string Header { get; } = header;
        public Alignment Alignment { get; } = alignment;
        public ConsoleColor? Color { get; } = color;
    }

    private readonly List<Column> _columns = [];
    private readonly List<string[]> _rows = [];
    private BoxStyle _style = BoxStyle.Single;
    private ConsoleColor _borderColor = ConsoleColor.Gray;

    public ConsoleTable SetStyle(BoxStyle style)
    {
        _style = style;
        return this;
    }

    public ConsoleTable SetBorderColor(ConsoleColor color)
    {
        _borderColor = color;
        return this;
    }

    public ConsoleTable AddColumn(string header, Alignment alignment = Alignment.Left, ConsoleColor? color = null)
    {
        _columns.Add(new Column(header, alignment, color));
        return this;
    }

    public ConsoleTable AddRow(params string[] values)
    {
        _rows.Add(values);
        return this;
    }

    public void Render()
    {
        var columnCount = _columns.Count;
        if (columnCount == 0) return;

        var widths = new int[columnCount];
        for (var i = 0; i < columnCount; i++)
        {
            var headerLen = ConsoleRender.StripMarkup(_columns[i].Header).Length;
            var maxRowLen = _rows.Count > 0
                ? _rows.Max(row => i < row.Length ? ConsoleRender.StripMarkup(row[i]).Length : 0)
                : 0;
            widths[i] = Math.Max(headerLen, maxRowLen);
        }

        var chars = _style.GetChars();
        var originalColor = System.Console.ForegroundColor;

        System.Console.ForegroundColor = _borderColor;
        System.Console.Write(chars.TopLeft);

        for (int i = 0; i < columnCount; i++)
        {
            System.Console.Write(new string(chars.Horizontal, widths[i] + 2));
            if (i < columnCount - 1)
            {
                System.Console.Write(chars.TopJoint);
            }
        }
        System.Console.WriteLine(chars.TopRight);

        System.Console.ForegroundColor = _borderColor;
        System.Console.Write(chars.Vertical);
        for (var i = 0; i < columnCount; i++)
        {
            System.Console.Write(" ");

            System.Console.ForegroundColor = _columns[i].Color ?? ConsoleColor.Yellow;
            WriteAligned(_columns[i].Header, _columns[i].Alignment, widths[i]);

            System.Console.ForegroundColor = _borderColor;
            System.Console.Write(" ");
            System.Console.Write(chars.Vertical);
        }
        System.Console.WriteLine();

        System.Console.ForegroundColor = _borderColor;
        System.Console.Write(chars.LeftJoint);
        for (var i = 0; i < columnCount; i++)
        {
            System.Console.Write(new string(chars.Horizontal, widths[i] + 2));
            if (i < columnCount - 1)
            {
                System.Console.Write(chars.CrossJoint);
            }
        }
        System.Console.WriteLine(chars.RightJoint);

        foreach (var row in _rows)
        {
            System.Console.ForegroundColor = _borderColor;
            System.Console.Write(chars.Vertical);
            for (var i = 0; i < columnCount; i++)
            {
                System.Console.Write(" ");
                System.Console.ResetColor();

                var cellText = i < row.Length ? row[i] : string.Empty;
                if (_columns[i].Color is { } cellColor)
                {
                    System.Console.ForegroundColor = cellColor;
                }

                WriteAligned(cellText, _columns[i].Alignment, widths[i]);

                System.Console.ForegroundColor = _borderColor;
                System.Console.Write(" ");
                System.Console.Write(chars.Vertical);
            }
            System.Console.WriteLine();
        }

        System.Console.ForegroundColor = _borderColor;
        System.Console.Write(chars.BottomLeft);
        for (var i = 0; i < columnCount; i++)
        {
            System.Console.Write(new string(chars.Horizontal, widths[i] + 2));
            if (i < columnCount - 1)
            {
                System.Console.Write(chars.BottomJoint);
            }
        }
        System.Console.WriteLine(chars.BottomRight);

        System.Console.ForegroundColor = originalColor;
    }

    private static void WriteAligned(string cellText, Alignment alignment, int width)
    {
        var plainLen = ConsoleRender.StripMarkup(cellText).Length;
        var totalSpaces = width - plainLen;

        var leftSpaces = 0;
        var rightSpaces = 0;

        switch (alignment)
        {
            case Alignment.Left:
                leftSpaces = 0;
                rightSpaces = totalSpaces;
                break;
            case Alignment.Center:
                leftSpaces = totalSpaces / 2;
                rightSpaces = totalSpaces - leftSpaces;
                break;
            case Alignment.Right:
                leftSpaces = totalSpaces;
                rightSpaces = 0;
                break;
            default:
               throw new ArgumentOutOfRangeException(nameof(alignment), alignment, null);
        }

        System.Console.Write(new string(' ', leftSpaces));
        ConsoleRender.WriteMarkup(cellText);
        System.Console.Write(new string(' ', rightSpaces));
    }
}
