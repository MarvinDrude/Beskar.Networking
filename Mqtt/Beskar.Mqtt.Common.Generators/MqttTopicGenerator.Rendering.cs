using System.Text;
using Beskar.Memory.Code;
using Microsoft.CodeAnalysis.CSharp;

namespace Beskar.Mqtt.Common.Generators;

public partial class MqttTopicGenerator
{
   private static string GenerateParserMethod(GeneratedMethodModel model)
   {
      if (model.Parameters.Count == 0) return "";

      var firstParam = model.Parameters[0];
      var firstParamType = firstParam.Type;

      var isByteSpan = firstParamType.Contains("System.ReadOnlySpan<byte>");
      var isCharSpan = firstParamType.Contains("System.ReadOnlySpan<char>");
      var isString = firstParamType == "string";

      if (!isByteSpan && !isCharSpan && !isString)
         return "";

      Span<char> initialBuffer = stackalloc char[2048];
      var writer = new CodeTextWriter(initialBuffer, stackalloc char[64]);

      try
      {
         var modifiers = model.MethodModifiers;
         var paramsList = string.Join(", ", model.Parameters.Select(p =>
         {
            var refKind = p.RefKind switch
            {
               "out" => "out ",
               "ref" => "ref ",
               "in" => "in ",
               _ => ""
            };
            return $"{refKind}{p.Type} {p.Name}";
         }));

         writer.WriteLineInterpolated($"{modifiers} {model.ReturnType} {model.MethodName}({paramsList})");
         writer.OpenBody();

         foreach (var param in model.Parameters.Skip(1))
            if (param.RefKind == "out")
               writer.WriteLineInterpolated($"{param.Name} = default!;");

         writer.WriteLine();

         writer.WriteLineInterpolated($"if ({firstParam.Name}.IsEmpty) return false;");
         writer.WriteLine();

         writer.WriteLine(isString
            ? $"ReadOnlySpan<char> remaining = {firstParam.Name}.AsSpan();"
            : $"var remaining = {firstParam.Name};");

         var segments = model.Pattern.Split('/');
         var totalSegments = segments.Length;

         for (var i = 0; i < totalSegments; i++)
         {
            var segment = segments[i];
            var isLast = i == totalSegments - 1;
            var nextIsWildcardHash = !isLast && segments[i + 1] == "#";

            if (segment == "#")
            {
               writer.WriteLine("return true;");
               break;
            }

            if (segment == "+")
            {
               if (isLast)
               {
                  writer.WriteLine(isByteSpan
                     ? "return !remaining.IsEmpty && remaining.IndexOf((byte)'/') < 0;"
                     : "return !remaining.IsEmpty && remaining.IndexOf('/') < 0;");
               }
               else
               {
                  writer.WriteLine(isByteSpan
                     ? $"int nextSlash_{i} = remaining.IndexOf((byte)'/');"
                     : $"int nextSlash_{i} = remaining.IndexOf('/');");

                  writer.WriteLineInterpolated($"if (nextSlash_{i} < 0) return false;");
                  writer.WriteLineInterpolated($"remaining = remaining.Slice(nextSlash_{i} + 1);");
               }

               continue;
            }

            if (segment.StartsWith("{") && segment.EndsWith("}"))
            {
               var placeholder = segment.Substring(1, segment.Length - 2);
               var colonIndex = placeholder.IndexOf(':');
               var paramName = colonIndex >= 0 ? placeholder[..colonIndex] : placeholder;

               var argParam = model.Parameters
                  .FirstOrDefault(p =>
                     p.RefKind == "out" && string.Equals(p.Name, paramName, StringComparison.OrdinalIgnoreCase));

               if (argParam.Name is null)
                  continue;

               var name = argParam.Name;
               var outParamType = argParam.Type;

               writer.WriteLineInterpolated($"// Extract dynamic segment for {{{paramName}}}");
               if (isLast)
               {
                  writer.WriteLine(isByteSpan
                     ? "if (remaining.IsEmpty || remaining.IndexOf((byte)'/') >= 0) return false;"
                     : "if (remaining.IsEmpty || remaining.IndexOf('/') >= 0) return false;");

                  writer.WriteLineInterpolated($"var rawVal_{i} = remaining;");
               }
               else
               {
                  if (isByteSpan)
                     writer.WriteLineInterpolated($"int nextSlash_{i} = remaining.IndexOf((byte)'/');");
                  else
                     writer.WriteLineInterpolated($"int nextSlash_{i} = remaining.IndexOf('/');");

                  writer.WriteLineInterpolated($"if (nextSlash_{i} <= 0) return false;");
                  writer.WriteLineInterpolated($"var rawVal_{i} = remaining.Slice(0, nextSlash_{i});");
                  writer.WriteLineInterpolated($"remaining = remaining.Slice(nextSlash_{i} + 1);");
               }

               var paramModel = model.Parameters.FirstOrDefault(p => p.Name == name);
               if (paramModel.IsEnum)
               {
                  if (isByteSpan)
                  {
                     writer.WriteLineInterpolated(
                        $"if (!System.Enum.TryParse(System.Text.Encoding.UTF8.GetString(rawVal_{i}.ToArray()), out {name})) return false;");
                  }
                  else
                  {
                     writer.WriteLineInterpolated(
                        $"if (!System.Enum.TryParse(rawVal_{i}.ToString(), out {name})) return false;");
                  }
               }
               else if (outParamType == "string")
               {
                  if (isByteSpan)
                  {
                     writer.WriteLineInterpolated($"{name} = Encoding.UTF8.GetString(rawVal_{i}.ToArray());");
                  }
                  else
                  {
                     writer.WriteLineInterpolated($"{name} = new string(rawVal_{i});");
                  }
               }
               else if (outParamType.Contains("System.ReadOnlySpan<char>"))
               {
                  if (isByteSpan)
                  {
                     writer.WriteLine(
                        "// Warning: allocating string to convert ReadOnlySpan<byte> to ReadOnlySpan<char>");
                     writer.WriteLineInterpolated($"{name} = Encoding.UTF8.GetString(rawVal_{i}.ToArray()).AsSpan();");
                  }
                  else
                  {
                     writer.WriteLineInterpolated($"{name} = rawVal_{i};");
                  }
               }
               else if (outParamType.Contains("System.ReadOnlySpan<byte>"))
               {
                  if (isByteSpan)
                  {
                     writer.WriteLineInterpolated($"{name} = rawVal_{i};");
                  }
                  else
                  {
                     writer.WriteLine("// Warning: allocating UTF8 bytes from ReadOnlySpan<char>");
                     writer.WriteLineInterpolated($"{name} = Encoding.UTF8.GetBytes(rawVal_{i}.ToArray());");
                  }
               }
               else
               {
                  if (isByteSpan)
                  {
                     writer.WriteLineInterpolated(
                        $"if (!Utf8Parser.TryParse(rawVal_{i}, out {name}, out _)) return false;");
                  }
                  else
                  {
                     writer.WriteLineInterpolated(
                        $"if (!{outParamType}.TryParse(rawVal_{i}, out {name})) return false;");
                  }
               }

               if (isLast) writer.WriteLine("return true;");
               continue;
            }

            var escapedSegment = SymbolDisplay.FormatLiteral(segment, true);
            writer.WriteLineInterpolated($"// Match literal segment: {escapedSegment}");
            if (isLast)
            {
               if (isByteSpan)
               {
                  writer.WriteLineInterpolated($"return remaining.SequenceEqual({escapedSegment}u8);");
               }
               else
               {
                  writer.WriteLineInterpolated($"return remaining.Equals({escapedSegment}, StringComparison.Ordinal);");
               }
            }
            else if (nextIsWildcardHash)
            {
               var escapedSegmentSlash = SymbolDisplay.FormatLiteral(segment + "/", true);
               if (isByteSpan)
               {
                  writer.WriteLineInterpolated($"if (remaining.SequenceEqual({escapedSegment}u8)) return true;");
                  writer.WriteLineInterpolated($"if (!remaining.StartsWith({escapedSegmentSlash}u8)) return false;");
               }
               else
               {
                  writer.WriteLineInterpolated(
                     $"if (remaining.Equals({escapedSegment}, StringComparison.Ordinal)) return true;");
                  writer.WriteLineInterpolated(
                     $"if (!remaining.StartsWith({escapedSegmentSlash}, StringComparison.Ordinal)) return false;");
               }

               var sliceLength = isByteSpan ? System.Text.Encoding.UTF8.GetByteCount(segment) + 1 : segment.Length + 1;
               writer.WriteLineInterpolated($"remaining = remaining.Slice({sliceLength});");
            }
            else
            {
               var escapedSegmentSlash = SymbolDisplay.FormatLiteral(segment + "/", true);
               if (isByteSpan)
               {
                  var sliceLength = System.Text.Encoding.UTF8.GetByteCount(segment) + 1;
                  writer.WriteLineInterpolated($"if (!remaining.StartsWith({escapedSegmentSlash}u8)) return false;");
                  writer.WriteLineInterpolated($"remaining = remaining.Slice({sliceLength});");
               }
               else
               {
                  writer.WriteLineInterpolated(
                     $"if (!remaining.StartsWith({escapedSegmentSlash}, StringComparison.Ordinal)) return false;");
                  writer.WriteLineInterpolated($"remaining = remaining.Slice({segment.Length + 1});");
               }
            }
         }

         writer.WriteLine("return false;");
         writer.CloseBody();

         return writer.ToString();
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static string GenerateFormatterMethod(GeneratedMethodModel model)
   {
      if (model.Parameters.Count == 0) return "";

      var firstParam = model.Parameters[0];
      var firstParamType = firstParam.Type;

      var isSpanDest = firstParamType.Contains("System.Span<char>") || firstParamType.Contains("System.Span<byte>");
      var isStringReturn = model.ReturnType == "string";

      Span<char> initialBuffer = stackalloc char[2048];
      var writer = new CodeTextWriter(initialBuffer, 4);

      try
      {
         var modifiers = model.MethodModifiers;

         var paramsList = string.Join(", ", model.Parameters.Select(p =>
         {
            var refKind = p.RefKind switch
            {
               "out" => "out ",
               "ref" => "ref ",
               "in" => "in ",
               _ => ""
            };
            return $"{refKind}{p.Type} {p.Name}";
         }));

         writer.WriteLineInterpolated($"{modifiers} {model.ReturnType} {model.MethodName}({paramsList})");
         writer.OpenBody();

         var segments = model.Pattern.Split('/');

         if (isStringReturn)
         {
            var formatParts = new List<string>();
            foreach (var segment in segments)
            {
               if (segment.StartsWith("{") && segment.EndsWith("}"))
               {
                  var placeholder = segment.Substring(1, segment.Length - 2);
                  var colonIndex = placeholder.IndexOf(':');
                  var paramName = colonIndex >= 0 ? placeholder.Substring(0, colonIndex) : placeholder;
                  formatParts.Add($"{{{paramName}}}");
               }
               else
               {
                  var escapedSegment = segment.Replace("\\", "\\\\").Replace("\"", "\\\"");
                  formatParts.Add(escapedSegment);
               }
            }

            var formatString = string.Join("/", formatParts);
            writer.WriteLineInterpolated($"return $\"{formatString}\";");
         }
         else if (isSpanDest)
         {
            var isByteSpan = firstParamType.Contains("System.Span<byte>");
            var charsWrittenParam = model.Parameters.LastOrDefault(p => p.RefKind == "out" && p.Type.Contains("int"));

            if (charsWrittenParam.Name is not null) writer.WriteLineInterpolated($"{charsWrittenParam.Name} = 0;");
            writer.WriteLineInterpolated($"var remainingDest = {firstParam.Name};");
            writer.WriteLine();

            for (var i = 0; i < segments.Length; i++)
            {
               var segment = segments[i];
               var isLast = i == segments.Length - 1;
               var suffix = isLast ? "" : "/";

               if (segment.StartsWith("{") && segment.EndsWith("}"))
               {
                  var placeholder = segment.Substring(1, segment.Length - 2);
                  var colonIndex = placeholder.IndexOf(':');
                  var paramName = colonIndex >= 0 ? placeholder.Substring(0, colonIndex) : placeholder;

                  var argParam = model.Parameters.FirstOrDefault(p =>
                     string.Equals(p.Name, paramName, StringComparison.OrdinalIgnoreCase));

                  if (argParam.Name is not null)
                  {
                     var argType = argParam.Type;
                     if (argType == "string" || argType.Contains("System.ReadOnlySpan<char>"))
                     {
                        if (isByteSpan)
                        {
                           writer.WriteLine("// Convert string/span to UTF8 bytes");
                           writer.WriteLineInterpolated(
                              $"int {paramName}Bytes = Encoding.UTF8.GetBytes({argParam.Name}, remainingDest);");
                           writer.WriteLineInterpolated($"remainingDest = remainingDest.Slice({paramName}Bytes);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {paramName}Bytes;");
                        }
                        else
                        {
                           var accessExpr = argType == "string" ? $"{argParam.Name}.AsSpan()" : argParam.Name;
                           writer.WriteLineInterpolated($"if (!{accessExpr}.TryCopyTo(remainingDest)) return false;");
                           writer.WriteLineInterpolated(
                              $"remainingDest = remainingDest.Slice({argParam.Name}.Length);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {argParam.Name}.Length;");
                        }
                     }
                     else if (argType.Contains("System.ReadOnlySpan<byte>"))
                     {
                        if (isByteSpan)
                        {
                           writer.WriteLineInterpolated(
                              $"if (!{argParam.Name}.TryCopyTo(remainingDest)) return false;");
                           writer.WriteLineInterpolated(
                              $"remainingDest = remainingDest.Slice({argParam.Name}.Length);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {argParam.Name}.Length;");
                        }
                        else
                        {
                           writer.WriteLine("// Warning: converting ReadOnlySpan<byte> to chars in formatting");
                           writer.WriteLineInterpolated(
                              $"int {paramName}Chars = Encoding.UTF8.GetChars({argParam.Name}, remainingDest);");
                           writer.WriteLineInterpolated($"remainingDest = remainingDest.Slice({paramName}Chars);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {paramName}Chars;");
                        }
                     }
                     else if (argParam.IsEnum)
                     {
                        if (isByteSpan)
                        {
                           writer.WriteLineInterpolated(
                              $"int {paramName}Bytes = System.Text.Encoding.UTF8.GetBytes({argParam.Name}.ToString(), remainingDest);");
                           writer.WriteLineInterpolated($"remainingDest = remainingDest.Slice({paramName}Bytes);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {paramName}Bytes;");
                        }
                        else
                        {
                           writer.WriteLineInterpolated(
                              $"if (!System.Enum.TryFormat({argParam.Name}, remainingDest, out int {paramName}Written)) return false;");
                           writer.WriteLineInterpolated($"remainingDest = remainingDest.Slice({paramName}Written);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {paramName}Written;");
                        }
                     }
                     else
                     {
                        if (isByteSpan)
                        {
                           writer.WriteLineInterpolated(
                              $"if (!System.Buffers.Text.Utf8Formatter.TryFormat({argParam.Name}, remainingDest, out int {paramName}Written)) return false;");
                           writer.WriteLineInterpolated($"remainingDest = remainingDest.Slice({paramName}Written);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {paramName}Written;");
                        }
                        else
                        {
                           writer.WriteLineInterpolated(
                              $"if (!{argParam.Name}.TryFormat(remainingDest, out int {paramName}Written)) return false;");
                           writer.WriteLineInterpolated($"remainingDest = remainingDest.Slice({paramName}Written);");
                           if (charsWrittenParam.Name is not null)
                              writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {paramName}Written;");
                        }
                     }
                  }
               }
               else
               {
                  var literalText = segment + suffix;
                  var escapedLiteralText = SymbolDisplay.FormatLiteral(literalText, true);
                  var literalLength = isByteSpan ? Encoding.UTF8.GetByteCount(literalText) : literalText.Length;

                  if (isByteSpan)
                  {
                     writer.WriteLineInterpolated(
                        $"if (!{escapedLiteralText}u8.TryCopyTo(remainingDest)) return false;");
                  }
                  else
                  {
                     writer.WriteLineInterpolated(
                        $"if (!{escapedLiteralText}.AsSpan().TryCopyTo(remainingDest)) return false;");
                  }

                  writer.WriteLineInterpolated($"remainingDest = remainingDest.Slice({literalLength});");

                  if (charsWrittenParam.Name is not null)
                     writer.WriteLineInterpolated($"{charsWrittenParam.Name} += {literalLength};");
               }

               if (!isLast && segment.StartsWith("{") && segment.EndsWith("}"))
               {
                  writer.WriteLine("if (remainingDest.IsEmpty) return false;");
                  writer.WriteLine(isByteSpan ? "remainingDest[0] = (byte)'/';" : "remainingDest[0] = '/';");
                  writer.WriteLine("remainingDest = remainingDest.Slice(1);");

                  if (charsWrittenParam.Name is not null)
                     writer.WriteLineInterpolated($"{charsWrittenParam.Name} += 1;");
               }
            }

            writer.WriteLine("return true;");
         }

         writer.CloseBody();

         // Generate additional helpers returning string and byte[] utilizing TextWriterIndentSlim and BufferWriter<byte>
         if (isSpanDest)
         {
            writer.WriteLine();

            var helperMethodName = model.MethodName;
            if (helperMethodName.StartsWith("TryFormat", StringComparison.OrdinalIgnoreCase))
            {
               helperMethodName = "Format" + helperMethodName.Substring(9);
            }
            else if (helperMethodName.StartsWith("Format", StringComparison.OrdinalIgnoreCase))
            {
               // Keep it as is
            }
            else
            {
               helperMethodName = "Format" + helperMethodName;
            }

            var bytesMethodName = helperMethodName + "ToBytes";

            var charsWrittenParam = model.Parameters.LastOrDefault(p => p.RefKind == "out" && p.Type.Contains("int"));
            var helperParams = model.Parameters.Skip(1).Where(p => p != charsWrittenParam).ToList();

            var helperParamsList = string.Join(", ", helperParams.Select(p =>
            {
               var refKind = p.RefKind switch
               {
                  "out" => "out ",
                  "ref" => "ref ",
                  "in" => "in ",
                  _ => ""
               };
               return $"{refKind}{p.Type} {p.Name}";
            }));

            var helperModifiers = modifiers.Replace("partial", "").Trim();

            // 1. Helper returning string using TextWriterIndentSlim
            writer.WriteLineInterpolated($"{helperModifiers} string {helperMethodName}({helperParamsList})");
            writer.OpenBody();
            writer.WriteLine("Span<char> initialBuffer = stackalloc char[256];");
            writer.WriteLine(
               "var writer = new Beskar.Memory.Writers.TextWriterIndentSlim(initialBuffer, stackalloc char[16]);");
            writer.WriteLine("try");
            writer.OpenBody();

            var formatParts = new List<string>();
            foreach (var segment in segments)
            {
               if (segment.StartsWith("{") && segment.EndsWith("}"))
               {
                  var placeholder = segment.Substring(1, segment.Length - 2);
                  var colonIndex = placeholder.IndexOf(':');
                  var paramName = colonIndex >= 0 ? placeholder.Substring(0, colonIndex) : placeholder;
                  formatParts.Add($"{{{paramName}}}");
               }
               else
               {
                  var escapedSegment = segment.Replace("\\", "\\\\").Replace("\"", "\\\"");
                  formatParts.Add(escapedSegment);
               }
            }

            var formatString = string.Join("/", formatParts);
            writer.WriteLineInterpolated($"writer.WriteInterpolated($\"{formatString}\");");
            writer.WriteLine("return writer.ToString();");
            writer.CloseBody();
            writer.WriteLine("finally");
            writer.OpenBody();
            writer.WriteLine("writer.Dispose();");
            writer.CloseBody();
            writer.CloseBody();

            writer.WriteLine();

            // 2. Helper returning byte[] using BufferWriter<byte>
            writer.WriteLineInterpolated($"{helperModifiers} byte[] {bytesMethodName}({helperParamsList})");
            writer.OpenBody();
            writer.WriteLine("Span<byte> initialBuffer = stackalloc byte[256];");
            writer.WriteLine("var writer = new Beskar.Memory.Writers.BufferWriter<byte>(initialBuffer);");
            writer.WriteLine("try");
            writer.OpenBody();

            for (var i = 0; i < segments.Length; i++)
            {
               var segment = segments[i];
               var isLast = i == segments.Length - 1;
               var suffix = isLast ? "" : "/";

               if (segment.StartsWith("{") && segment.EndsWith("}"))
               {
                  var placeholder = segment.Substring(1, segment.Length - 2);
                  var colonIndex = placeholder.IndexOf(':');
                  var paramName = colonIndex >= 0 ? placeholder.Substring(0, colonIndex) : placeholder;

                  var argParam = model.Parameters.FirstOrDefault(p =>
                     string.Equals(p.Name, paramName, StringComparison.OrdinalIgnoreCase));

                  if (argParam.Name is not null)
                  {
                     var argType = argParam.Type;
                     if (argType == "string" || argType.Contains("System.ReadOnlySpan<char>"))
                     {
                        writer.WriteLineInterpolated(
                           $"int {paramName}ByteCount = System.Text.Encoding.UTF8.GetByteCount({argParam.Name});");
                        writer.WriteLineInterpolated(
                           $"System.Text.Encoding.UTF8.GetBytes({argParam.Name}, writer.AcquireSpan({paramName}ByteCount));");
                     }
                     else if (argType.Contains("System.ReadOnlySpan<byte>"))
                     {
                        writer.WriteLineInterpolated($"writer.Write({argParam.Name});");
                     }
                     else if (argParam.IsEnum)
                     {
                        writer.WriteLineInterpolated(
                           $"int {paramName}ByteCount = System.Text.Encoding.UTF8.GetByteCount({argParam.Name}.ToString());");
                        writer.WriteLineInterpolated(
                           $"System.Text.Encoding.UTF8.GetBytes({argParam.Name}.ToString(), writer.AcquireSpan({paramName}ByteCount));");
                     }
                     else
                     {
                        writer.WriteLineInterpolated($"int {paramName}Written;");
                        writer.WriteLineInterpolated(
                           $"if (!System.Buffers.Text.Utf8Formatter.TryFormat({argParam.Name}, writer.AcquireSpan(64, movePosition: false), out {paramName}Written))");
                        writer.OpenBody();
                        writer.WriteLineInterpolated(
                           $"throw new System.InvalidOperationException(\"Failed to UTF-8 format topic parameter '{paramName}'.\");");
                        writer.CloseBody();
                        writer.WriteLineInterpolated($"writer.Advance({paramName}Written);");
                     }
                  }
               }
               else
               {
                  var literalText = segment + suffix;
                  var escapedLiteralText = SymbolDisplay.FormatLiteral(literalText, true);
                  writer.WriteLineInterpolated($"writer.Write({escapedLiteralText}u8);");
               }

               if (!isLast && segment.StartsWith("{") && segment.EndsWith("}"))
               {
                  writer.WriteLine("writer.Add((byte)'/');");
               }
            }

            writer.WriteLine("return writer.WrittenSpan.ToArray();");
            writer.CloseBody();
            writer.WriteLine("finally");
            writer.OpenBody();
            writer.WriteLine("writer.Dispose();");
            writer.CloseBody();
            writer.CloseBody();
         }

         return writer.ToString();
      }
      finally
      {
         writer.Dispose();
      }
   }
}
