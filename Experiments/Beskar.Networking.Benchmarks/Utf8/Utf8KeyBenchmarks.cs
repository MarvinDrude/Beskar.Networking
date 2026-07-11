using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Beskar.Networking.Benchmarks.Utf8;

[Config(typeof(Config))]
[MemoryDiagnoser]
public class Utf8KeyBenchmarks
{
   private class Config : ManualConfig
   {
      public Config()
      {
         AddJob(Job.Default
            .WithToolchain(new InProcessEmitToolchain(new InProcessEmitSettings()))
            .WithLaunchCount(1)
            .WithWarmupCount(2));
      }
   }

   private Dictionary<string, int> _stringDict = null!;
   private Dictionary<byte[], int> _byteArrayDict = null!;

   private string[] _stringKeys = null!;
   private byte[][] _utf8Keys = null!;
   private byte[] _combinedBuffer = null!;
   private (int Offset, int Length)[] _keyRanges = null!;

   [Params(10, 100)] public int KeyCount { get; set; }

   [GlobalSetup]
   public void Setup()
   {
      _stringKeys = new string[KeyCount];
      _utf8Keys = new byte[KeyCount][];
      _keyRanges = new (int Offset, int Length)[KeyCount];

      var random = new Random(42);
      var totalBytes = 0;

      for (var i = 0; i < KeyCount; i++)
      {
         var length = random.Next(10, 41);
         var sb = new StringBuilder(length);

         for (var j = 0; j < length; j++)
         {
            sb.Append((char)random.Next('a', 'z' + 1));
         }

         _stringKeys[i] = sb.ToString();
         _utf8Keys[i] = Encoding.UTF8.GetBytes(_stringKeys[i]);
         totalBytes += _utf8Keys[i].Length;
      }

      _combinedBuffer = new byte[totalBytes];
      var currentOffset = 0;

      for (var i = 0; i < KeyCount; i++)
      {
         var keyBytes = _utf8Keys[i];
         Array.Copy(keyBytes, 0, _combinedBuffer, currentOffset, keyBytes.Length);
         _keyRanges[i] = (currentOffset, keyBytes.Length);
         currentOffset += keyBytes.Length;
      }

      _stringDict = new Dictionary<string, int>(StringComparer.Ordinal);
      _byteArrayDict = new Dictionary<byte[], int>(ByteArrayEqualityComparer.Instance);

      for (var i = 0; i < KeyCount; i++)
      {
         _stringDict.Add(_stringKeys[i], i);
         _byteArrayDict.Add(_utf8Keys[i], i);
      }
   }

   #region Lookups

   [Benchmark(Baseline = true)]
   public int LookupString_WithAllocation()
   {
      var sum = 0;
      var buffer = _combinedBuffer.AsSpan();

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         var key = Encoding.UTF8.GetString(bytesSpan);
         if (_stringDict.TryGetValue(key, out var val))
         {
            sum += val;
         }
      }

      return sum;
   }

   [Benchmark]
   public int LookupString_WithAlternateLookup()
   {
      var sum = 0;
      var buffer = _combinedBuffer.AsSpan();
      var alternateLookup = _stringDict.GetAlternateLookup<ReadOnlySpan<char>>();

      Span<char> charBuffer = stackalloc char[128];

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         var charCount = Encoding.UTF8.GetChars(bytesSpan, charBuffer);
         var charSpan = charBuffer.Slice(0, charCount);

         if (alternateLookup.TryGetValue(charSpan, out int val))
         {
            sum += val;
         }
      }

      return sum;
   }

   [Benchmark]
   public int LookupBytes_WithAllocation()
   {
      var sum = 0;
      var buffer = _combinedBuffer.AsSpan();

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         var key = bytesSpan.ToArray();
         if (_byteArrayDict.TryGetValue(key, out var val))
         {
            sum += val;
         }
      }

      return sum;
   }

   [Benchmark]
   public int LookupBytes_WithAlternateLookup()
   {
      var sum = 0;
      var buffer = _combinedBuffer.AsSpan();
      var alternateLookup = _byteArrayDict.GetAlternateLookup<ReadOnlySpan<byte>>();

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         if (alternateLookup.TryGetValue(bytesSpan, out var val))
         {
            sum += val;
         }
      }

      return sum;
   }

   #endregion

   #region Adds

   [Benchmark]
   public Dictionary<string, int> AddString_WithAllocation()
   {
      var dict = new Dictionary<string, int>(StringComparer.Ordinal);
      var buffer = _combinedBuffer.AsSpan();

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         var key = Encoding.UTF8.GetString(bytesSpan);
         dict.TryAdd(key, i);
      }

      return dict;
   }

   [Benchmark]
   public Dictionary<string, int> AddString_WithAlternateLookup()
   {
      var dict = new Dictionary<string, int>(StringComparer.Ordinal);
      var alternateLookup = dict.GetAlternateLookup<ReadOnlySpan<char>>();
      var buffer = _combinedBuffer.AsSpan();

      Span<char> charBuffer = stackalloc char[128];

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         var charCount = Encoding.UTF8.GetChars(bytesSpan, charBuffer);
         var charSpan = charBuffer[..charCount];

         alternateLookup.TryAdd(charSpan, i);
      }

      return dict;
   }

   [Benchmark]
   public Dictionary<byte[], int> AddBytes_WithAllocation()
   {
      var dict = new Dictionary<byte[], int>(ByteArrayEqualityComparer.Instance);
      var buffer = _combinedBuffer.AsSpan();

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         var key = bytesSpan.ToArray();
         dict.TryAdd(key, i);
      }

      return dict;
   }

   [Benchmark]
   public Dictionary<byte[], int> AddBytes_WithAlternateLookup()
   {
      var dict = new Dictionary<byte[], int>(ByteArrayEqualityComparer.Instance);
      var alternateLookup = dict.GetAlternateLookup<ReadOnlySpan<byte>>();
      var buffer = _combinedBuffer.AsSpan();

      for (var i = 0; i < _keyRanges.Length; i++)
      {
         var range = _keyRanges[i];
         var bytesSpan = buffer.Slice(range.Offset, range.Length);

         alternateLookup.TryAdd(bytesSpan, i);
      }

      return dict;
   }

   #endregion
}

public sealed class ByteArrayEqualityComparer : IEqualityComparer<byte[]>,
   IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
{
   public static readonly ByteArrayEqualityComparer Instance = new();

   public bool Equals(byte[]? x, byte[]? y)
   {
      if (ReferenceEquals(x, y)) return true;
      if (x is null || y is null) return false;
      return x.AsSpan().SequenceEqual(y);
   }

   public int GetHashCode(byte[] obj)
   {
      return GetHashCode(obj.AsSpan());
   }

   public bool Equals(ReadOnlySpan<byte> alternate, byte[] other)
   {
      return alternate.SequenceEqual(other);
   }

   public int GetHashCode(ReadOnlySpan<byte> alternate)
   {
      var hashCode = new HashCode();
      hashCode.AddBytes(alternate);
      return hashCode.ToHashCode();
   }

   public byte[] Create(ReadOnlySpan<byte> alternate)
   {
      return [.. alternate];
   }
}
