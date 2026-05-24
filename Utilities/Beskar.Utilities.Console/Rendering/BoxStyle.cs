namespace Beskar.Utilities.Console.Rendering;

public enum BoxStyle
{
    Single,
    Double,
    Rounded,
    Heavy,
    Ascii
}

public readonly record struct BorderChars(
    char TopLeft,
    char TopRight,
    char BottomLeft,
    char BottomRight,
    char Horizontal,
    char Vertical,
    char TopJoint,
    char BottomJoint,
    char LeftJoint,
    char RightJoint,
    char CrossJoint
);

public static class BoxStyleExtensions
{
    public static BorderChars GetChars(this BoxStyle style) => style switch
    {
        BoxStyle.Single => new BorderChars('┌', '┐', '└', '┘', '─', '│', '┬', '┴', '├', '┤', '┼'),
        BoxStyle.Double => new BorderChars('╔', '╗', '╚', '╝', '═', '║', '╦', '╩', '╠', '╣', '╬'),
        BoxStyle.Rounded => new BorderChars('╭', '╮', '╰', '╯', '─', '│', '┬', '┴', '├', '┤', '┼'),
        BoxStyle.Heavy => new BorderChars('┏', '┓', '┗', '┛', '━', '┃', '┳', '┻', '┣', '┫', '╋'),
        BoxStyle.Ascii => new BorderChars('+', '+', '+', '+', '-', '|', '+', '+', '+', '+', '+'),
        _ => throw new ArgumentOutOfRangeException(nameof(style), style, null)
    };
}
