namespace Beskar.Mqtt.Common.Generators;

public interface ISignalAwaiter<TResponseMessage> : ISignalAwaiter
{
   public bool TryComplete(in TResponseMessage message);
}

public interface ISignalAwaiter : IDisposable
{
   public ISignalAwaiter? Next { get; set; }

   public ushort Identifier { get; }
   public Type MessageType { get; }
   public bool IsPruned { get; }

   public void Fail(Exception exception);
   public void Cancel();

   public bool TryComplete<TIncoming>(in TIncoming message);

   public bool IsPending { get; }
   public bool IsCompletedSuccessfully { get; }
   public void OnPruned();
}
