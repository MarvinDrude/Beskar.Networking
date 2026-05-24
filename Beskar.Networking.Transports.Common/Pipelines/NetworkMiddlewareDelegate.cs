using System.IO.Pipelines;

namespace Beskar.Networking.Transports.Common.Pipelines;

public delegate Task NetworkMiddlewareDelegate(IDuplexPipe pipe, Func<Task> next);
