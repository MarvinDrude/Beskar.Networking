using System;
using System.Buffers;

namespace Beskar.Mqtt.Common.Tests.Helpers;


public class MemoryBuffer(int initialCapacity = 1024) : IBufferWriter<byte>
{
    private byte[] _buffer = new byte[initialCapacity];
    private int _position = 0;

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _position);
    public ReadOnlySequence<byte> WrittenSequence => new(_buffer.AsMemory(0, _position));

    public void Clear()
    {
        _position = 0;
    }

    public void Advance(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        if (_position + count > _buffer.Length) throw new InvalidOperationException("Cannot advance past buffer capacity.");
        _position += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_position);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_position);
    }

    private void EnsureCapacity(int sizeHint)
    {
        var needed = _position + (sizeHint == 0 ? 256 : sizeHint);
        if (needed <= _buffer.Length) return;

        var newSize = Math.Max(_buffer.Length * 2, needed);
        var newBuffer = new byte[newSize];

        Array.Copy(_buffer, newBuffer, _position);
        _buffer = newBuffer;
    }
}
