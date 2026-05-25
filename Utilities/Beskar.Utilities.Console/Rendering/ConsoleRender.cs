using System.Text.RegularExpressions;
using Beskar.Utilities.Console.Constants;

namespace Beskar.Utilities.Console.Rendering;

public static partial class ConsoleRender
{
   private static readonly Dictionary<string, ConsoleColor> ColorAliases = new(StringComparer.OrdinalIgnoreCase)
   {
      { "info", ColorConstants.Info },
      { "success", ColorConstants.Success },
      { "error", ColorConstants.Error },
      { "warning", ColorConstants.Warning },
      { "client", ColorConstants.Client },
      { "server", ColorConstants.Server }
   };

   public static void WriteMarkup(string markup) => RenderMarkup(markup, false);

   public static void WriteMarkupLine(string markup) => RenderMarkup(markup, true);

   /// <summary>
   /// Strips all valid markup tags from a string to compute plain text lengths for positioning.
   /// </summary>
   public static string StripMarkup(string markup)
   {
      if (string.IsNullOrEmpty(markup)) return "";
      return MyRegex().Replace(markup, m =>
      {
         var content = m.Groups[1].Value;
         if (content == "/" || ColorAliases.ContainsKey(content) || Enum.TryParse<ConsoleColor>(content, true, out _))
         {
            return "";
         }

         return m.Value;
      });
   }

   private static void RenderMarkup(string markup, bool writeNewLine)
   {
      if (string.IsNullOrEmpty(markup))
      {
         if (writeNewLine) System.Console.WriteLine();
         return;
      }

      var colorStack = new Stack<ConsoleColor>();
      var originalColor = System.Console.ForegroundColor;

      var i = 0;
      while (i < markup.Length)
      {
         var nextOpen = markup.IndexOf('[', i);
         if (nextOpen == -1)
         {
            System.Console.Write(markup[i..]);
            break;
         }

         if (nextOpen > i)
         {
            System.Console.Write(markup[i..nextOpen]);
         }

         var nextClose = markup.IndexOf(']', nextOpen);
         if (nextClose == -1)
         {
            System.Console.Write(markup[nextOpen..]);
            break;
         }

         var tag = markup[(nextOpen + 1)..nextClose];
         var isClosing = tag.StartsWith('/');
         var tagContent = isClosing ? tag[1..] : tag;

         if (tagContent == "" && isClosing) // Support `[/]`
         {
            if (colorStack.Count > 0)
            {
               colorStack.Pop();
            }

            System.Console.ForegroundColor = colorStack.Count > 0 ? colorStack.Peek() : originalColor;
            i = nextClose + 1;
            continue;
         }

         if (ColorAliases.TryGetValue(tagContent, out var parsedColor) ||
             Enum.TryParse(tagContent, true, out parsedColor))
         {
            if (isClosing)
            {
               if (colorStack.Count > 0 && colorStack.Peek() == parsedColor)
               {
                  colorStack.Pop();
               }

               System.Console.ForegroundColor = colorStack.Count > 0 ? colorStack.Peek() : originalColor;
            }
            else
            {
               colorStack.Push(parsedColor);
               System.Console.ForegroundColor = parsedColor;
            }

            i = nextClose + 1;
         }
         else
         {
            // Not a valid color tag, treat as literal text
            System.Console.Write(markup[nextOpen..(nextClose + 1)]);
            i = nextClose + 1;
         }
      }

      System.Console.ForegroundColor = originalColor;
      if (writeNewLine)
      {
         System.Console.WriteLine();
      }
   }

   // Predefined Logger helpers
   public static void Info(string message) => WriteMarkupLine($"[info][INFO][/info] {message}");
   public static void Success(string message) => WriteMarkupLine($"[success][✔][/success] {message}");
   public static void Warning(string message) => WriteMarkupLine($"[warning][⚠][/warning] {message}");
   public static void Error(string message) => WriteMarkupLine($"[error][✘][/error] {message}");
   public static void Client(string message) => WriteMarkupLine($"[client][CLIENT][/client] {message}");
   public static void Server(string message) => WriteMarkupLine($"[server][SERVER][/server] {message}");

   [GeneratedRegex(@"\[/?([a-zA-Z]+|/)\]")]
   private static partial Regex MyRegex();
}
