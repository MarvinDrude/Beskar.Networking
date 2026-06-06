using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Memory.Writers;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
