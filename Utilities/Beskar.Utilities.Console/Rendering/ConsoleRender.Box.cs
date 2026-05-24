namespace Beskar.Utilities.Console.Rendering;

public enum Alignment
{
    Left,
    Center,
    Right
}

public static partial class ConsoleRender
{
    /// <summary>
    /// Draws a beautiful structured header with automatic centering and spacing.
    /// </summary>
    public static void DrawHeader(string title, string? subtitle = null, BoxStyle style = BoxStyle.Double, ConsoleColor borderColor = ConsoleColor.DarkYellow)
    {
        var list = new List<string> { $"[yellow]{title.ToUpper()}[/]" };
        if (!string.IsNullOrEmpty(subtitle))
        {
            list.Add(subtitle);
        }

        System.Console.WriteLine();
        DrawBox([.. list], style, borderColor, paddingLeftRight: 5, paddingTopBottom: 1, alignment: Alignment.Center);
        System.Console.WriteLine();
    }

    /// <summary>
    /// Draws a highly configurable box/panel with border characters, custom styling, alignment, and title embedding.
    /// </summary>
    public static void DrawBox(
        string[] lines,
        BoxStyle style = BoxStyle.Single,
        ConsoleColor borderColor = ConsoleColor.Gray,
        int paddingLeftRight = 2,
        int paddingTopBottom = 0,
        string? title = null,
        ConsoleColor titleColor = ConsoleColor.Yellow,
        Alignment alignment = Alignment.Left)
    {
        var chars = style.GetChars();
        var maxContentLength = lines.Length > 0 ? lines.Max(line => StripMarkup(line).Length) : 0;

        var titleLen = title is not null ? StripMarkup(title).Length : 0;
        var minContentWidth = title is not null ? titleLen + 4 : 0;
        var contentWidth = Math.Max(maxContentLength, minContentWidth);

        var innerWidth = contentWidth + (2 * paddingLeftRight);
        var outerWidth = innerWidth + 2;
        var horizontalCount = outerWidth - 2;

        var originalColor = System.Console.ForegroundColor;

        System.Console.ForegroundColor = borderColor;
        System.Console.Write(chars.TopLeft);

        if (title is not null)
        {
            var titleText = $" {title} ";
            var totalTitleLen = StripMarkup(titleText).Length;
            var leftCount = (horizontalCount - totalTitleLen) / 2;
            var rightCount = horizontalCount - totalTitleLen - leftCount;

            System.Console.Write(new string(chars.Horizontal, leftCount));
            System.Console.ForegroundColor = titleColor;
            WriteMarkup(titleText);
            System.Console.ForegroundColor = borderColor;
            System.Console.Write(new string(chars.Horizontal, rightCount));
        }
        else
        {
            System.Console.Write(new string(chars.Horizontal, horizontalCount));
        }

        System.Console.WriteLine(chars.TopRight);

        for (var p = 0; p < paddingTopBottom; p++)
        {
            System.Console.ForegroundColor = borderColor;
            System.Console.Write(chars.Vertical);
            System.Console.Write(new string(' ', innerWidth));
            System.Console.ForegroundColor = borderColor;
            System.Console.WriteLine(chars.Vertical);
        }

        foreach (var line in lines)
        {
            var plainText = StripMarkup(line);
            var totalSpaces = innerWidth - plainText.Length;

            var leftSpaces = 0;
            var rightSpaces = 0;

            switch (alignment)
            {
                case Alignment.Left:
                    leftSpaces = paddingLeftRight;
                    rightSpaces = totalSpaces - leftSpaces;
                    break;
                case Alignment.Center:
                    leftSpaces = totalSpaces / 2;
                    rightSpaces = totalSpaces - leftSpaces;
                    break;
                case Alignment.Right:
                    rightSpaces = paddingLeftRight;
                    leftSpaces = totalSpaces - rightSpaces;
                    break;
            }

            System.Console.ForegroundColor = borderColor;
            System.Console.Write(chars.Vertical);

            System.Console.Write(new string(' ', leftSpaces));
            WriteMarkup(line);
            System.Console.Write(new string(' ', rightSpaces));

            System.Console.ForegroundColor = borderColor;
            System.Console.WriteLine(chars.Vertical);
        }

        for (var p = 0; p < paddingTopBottom; p++)
        {
            System.Console.ForegroundColor = borderColor;
            System.Console.Write(chars.Vertical);
            System.Console.Write(new string(' ', innerWidth));
            System.Console.ForegroundColor = borderColor;
            System.Console.WriteLine(chars.Vertical);
        }

        System.Console.ForegroundColor = borderColor;
        System.Console.Write(chars.BottomLeft);
        System.Console.Write(new string(chars.Horizontal, horizontalCount));
        System.Console.WriteLine(chars.BottomRight);

        System.Console.ForegroundColor = originalColor;
    }
}
