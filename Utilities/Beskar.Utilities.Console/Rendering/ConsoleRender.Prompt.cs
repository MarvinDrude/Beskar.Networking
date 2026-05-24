namespace Beskar.Utilities.Console.Rendering;

public readonly record struct PromptChoice(string Key, string Label);

public static partial class ConsoleRender
{
    /// <summary>
    /// Prompts the user for a text value, with optional default fallbacks.
    /// </summary>
    public static string AskString(string promptText, string? defaultValue = null, bool allowEmpty = false)
    {
        var originalColor = System.Console.ForegroundColor;
        while (true)
        {
            System.Console.ForegroundColor = ConsoleColor.Cyan;
            System.Console.Write(promptText);

            if (defaultValue is not null)
            {
                System.Console.ForegroundColor = ConsoleColor.DarkGray;
                System.Console.Write($" [default: {defaultValue}]");
            }

            System.Console.ForegroundColor = ConsoleColor.DarkGray;
            System.Console.Write(" > ");
            System.Console.ForegroundColor = originalColor;

            var input = System.Console.ReadLine()?.Trim();
            if (string.IsNullOrEmpty(input))
            {
                if (defaultValue is not null)
                {
                    return defaultValue;
                }
                if (allowEmpty)
                {
                    return string.Empty;
                }

                Error("Input cannot be empty. Please try again.");
                continue;
            }
            return input;
        }
    }

    /// <summary>
    /// Prompts the user for a Yes/No confirmation.
    /// </summary>
    public static bool Confirm(string promptText, bool defaultValue = true)
    {
        var originalColor = System.Console.ForegroundColor;
        var optionsText = defaultValue ? "Y/n" : "y/N";

        while (true)
        {
            System.Console.ForegroundColor = ConsoleColor.Cyan;
            System.Console.Write(promptText);

            System.Console.ForegroundColor = ConsoleColor.DarkGray;
            System.Console.Write($" ({optionsText})");
            System.Console.Write(" > ");
            System.Console.ForegroundColor = originalColor;

            var input = System.Console.ReadLine()?.Trim().ToLower();
            if (string.IsNullOrEmpty(input))
            {
                return defaultValue;
            }

            if (input is "y" or "yes") return true;
            if (input is "n" or "no") return false;

            Error("Invalid response. Please enter 'y' or 'n'.");
        }
    }

    /// <summary>
    /// Prompts the user for a choice from a list of option-key pairs (e.g. (t) TCP / (q) QUIC).
    /// </summary>
    public static string AskChoice(string promptText, PromptChoice[] choices, string? defaultChoice = null, bool vertical = false)
    {
        var originalColor = System.Console.ForegroundColor;

        if (vertical)
        {
            System.Console.ForegroundColor = ConsoleColor.Cyan;
            System.Console.WriteLine($"{promptText}:");
            foreach (var choice in choices)
            {
                WriteMarkupLine($"  [yellow]{choice.Key.ToLower()}[/] - {choice.Label}");
            }
            System.Console.WriteLine();
        }

        var formattedChoicesList = choices.Select(c => $"({c.Key.ToLower()}) {c.Label}");
        var choicesPrompt = string.Join(" / ", formattedChoicesList);

        while (true)
        {
            System.Console.ForegroundColor = ConsoleColor.Cyan;
            if (vertical)
            {
                System.Console.Write("Select action");
            }
            else
            {
                System.Console.Write($"{promptText} ({choicesPrompt})");
            }

            if (defaultChoice is not null)
            {
                System.Console.ForegroundColor = ConsoleColor.DarkGray;
                System.Console.Write($" [default: {defaultChoice.ToLower()}]");
            }

            System.Console.ForegroundColor = ConsoleColor.DarkGray;
            System.Console.Write(" > ");
            System.Console.ForegroundColor = originalColor;

            var input = System.Console.ReadLine()?.Trim().ToLower();
            if (string.IsNullOrEmpty(input))
            {
                if (defaultChoice is not null)
                {
                    return defaultChoice.ToLower();
                }
                Error("Please select one of the options.");
                continue;
            }

            var matchedChoice = choices.FirstOrDefault(c => string.Equals(c.Key, input, StringComparison.OrdinalIgnoreCase));
            if (matchedChoice.Key is not null)
            {
                return matchedChoice.Key.ToLower();
            }

            Error($"Invalid option. Please enter one of the keys: {string.Join(", ", choices.Select(c => c.Key.ToLower()))}");
        }
    }

    /// <summary>
    /// Prompts the user for a choice from simple option strings, automatically mapping keys to first letters.
    /// </summary>
    public static string AskChoice(string promptText, string[] choices, string? defaultChoice = null, bool vertical = false)
    {
        var promptChoices = choices.Select(c => new PromptChoice(c[..1].ToLower(), c)).ToArray();
        return AskChoice(promptText, promptChoices, defaultChoice, vertical);
    }
}
