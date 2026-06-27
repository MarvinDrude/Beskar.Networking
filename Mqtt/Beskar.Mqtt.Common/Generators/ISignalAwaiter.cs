namespace Beskar.Mqtt.Common.Generators;

public interface ISignalAwaiter<in TResponseMessage> : ISignalAwaiter
{
   public bool TryComplete(TResponseMessage message);
}

public interface ISignalAwaiter : IDisposable
{
   public ISignalAwaiter? Next { get; set; }

   public ushort Identifier { get; }
   public Type MessageType { get; }

   public void Fail(Exception exception);
   public void Cancel();

   public bool TryComplete<TIncoming>(TIncoming message);

   public bool IsPending { get; }
   public void OnPruned();
}

