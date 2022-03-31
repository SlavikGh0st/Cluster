using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartRoundRobinClusterClient : SmartClusterClientBase
    {
        public SmartRoundRobinClusterClient(IEnumerable<string> replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var replicaCounter = ReplicaAddresses.Count;
            var averageTimeout = timeout / replicaCounter;

            foreach (var uri in ReplicaAddresses
                .OrderBy(entry => entry.Value)
                .Select(entry => entry.Key))
            {
                var request = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {request.RequestUri}");
                var resultTask = ProcessRequestAsync(request, uri);

                await Task.WhenAny(resultTask, Task.Delay(averageTimeout));
                if (resultTask.IsCompletedSuccessfully)
                    return resultTask.Result;

                if (resultTask.IsFaulted)
                {
                    ReplicaAddresses[uri] = TimeSpan.MaxValue;
                    averageTimeout = timeout / (--replicaCounter);
                }
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}