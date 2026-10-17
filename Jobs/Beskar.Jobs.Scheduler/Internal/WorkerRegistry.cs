using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Common.Telemetry;

namespace Beskar.Jobs.Scheduler.Internal;

internal sealed class WorkerRegistry
{
   private readonly Dictionary<string, WorkerNodeState> _workers = new(StringComparer.Ordinal);
   private readonly Lock _lock = new();
   private uint _roundRobinCounter;

   public int OnlineWorkerCount
   {
      get
      {
         lock (_lock)
         {
            var count = 0;

            foreach (var w in _workers.Values)
            {
               if (w.IsOnline) count++;
            }

            return count;
         }
      }
   }

   /// <summary>
   /// Updates worker state from a heartbeat payload. Returns true if the worker transitioned from Online to Offline.
   /// </summary>
   public bool UpdateFromHeartbeat(WorkerHeartbeatPayload payload, out bool transitionedToOnline)
   {
      transitionedToOnline = false;
      lock (_lock)
      {
         var existed = _workers.TryGetValue(payload.WorkerId, out var state);
         var wasOnline = existed && state!.IsOnline;

         if (!existed)
         {
            state = new WorkerNodeState { WorkerId = payload.WorkerId };
            _workers[payload.WorkerId] = state;
         }

         state!.IsOnline = payload.IsOnline;
         state.Queues = [with(StringComparer.Ordinal), .. payload.Queues ?? ["default"]];
         state.SupportedJobTypes = [with(StringComparer.Ordinal), .. payload.SupportedJobTypes ?? []];
         state.ActiveJobs = payload.ActiveJobs;
         state.MaxConcurrency = Math.Max(1, payload.MaxConcurrency);
         state.LastHeartbeatUtc = payload.TimestampUtc;

         if (!wasOnline && payload.IsOnline)
         {
            transitionedToOnline = true;
            JobMetrics.RecordWorkersOnlineChange(1);
         }
         else if (wasOnline && !payload.IsOnline)
         {
            JobMetrics.RecordWorkersOnlineChange(-1);
            return true;
         }

         return false;
      }
   }

   /// <summary>
   /// Returns candidate online workers capable of running <paramref name="jobType"/> on <paramref name="queue"/>,
   /// ordered by lowest load factor and round-robin tie-breaking.
   /// </summary>
   public List<string> SelectCandidateWorkers(string queue, string jobType)
   {
      lock (_lock)
      {
         var candidates = new List<WorkerNodeState>();
         foreach (var w in _workers.Values)
         {
            if (!w.IsOnline || w.AvailableSlots <= 0)
            {
               continue;
            }

            if (!w.Queues.Contains(queue))
            {
               continue;
            }

            if (w.SupportedJobTypes.Count > 0
                && !w.SupportedJobTypes.Contains(jobType))
            {
               continue;
            }

            candidates.Add(w);
         }

         if (candidates.Count == 0)
         {
            return [];
         }

         var rr = _roundRobinCounter++;
         candidates.Sort((a, b) =>
         {
            var cmp = a.LoadFactor.CompareTo(b.LoadFactor);
            if (cmp != 0) return cmp;

            var hashA = ((uint)StringComparer.Ordinal.GetHashCode(a.WorkerId) + rr) & 0x7FFFFFFF;
            var hashB = ((uint)StringComparer.Ordinal.GetHashCode(b.WorkerId) + rr) & 0x7FFFFFFF;

            return hashA.CompareTo(hashB);
         });

         var ids = new List<string>(candidates.Count);
         foreach (var c in candidates)
         {
            ids.Add(c.WorkerId);
         }

         return ids;
      }
   }

   public void IncrementWorkerActiveCount(string workerId)
   {
      lock (_lock)
      {
         if (_workers.TryGetValue(workerId, out var state))
         {
            state.ActiveJobs++;
         }
      }
   }

   public void DecrementWorkerActiveCount(string workerId)
   {
      lock (_lock)
      {
         if (_workers.TryGetValue(workerId, out var state) && state.ActiveJobs > 0)
         {
            state.ActiveJobs--;
         }
      }
   }
}
