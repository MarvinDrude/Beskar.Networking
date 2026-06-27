using Beskar.Memory.Pools;

namespace Beskar.Mqtt.Common.Generators;

public static class SignalAwaiterPool<TResponseMessage>
{
   private static readonly ObjectPool<SignalAwaiter<TResponseMessage>> _pool = new(
      new ObjectPoolOptions<SignalAwaiter<TResponseMessage>>()
      {
         FactoryFunc = () => new SignalAwaiter<TResponseMessage>(0),
         InitialSize = 0,
         MaxSize = 512
      });

   public static SignalAwaiter<TResponseMessage> Get(ushort identifier, SignalBroker broker)
   {
      var awaiter = _pool.Get(null);
      awaiter.Reset(identifier, broker);

      return awaiter;
   }

   public static void Return(SignalAwaiter<TResponseMessage> awaiter)
   {
      _pool.Return(awaiter);
   }
}
