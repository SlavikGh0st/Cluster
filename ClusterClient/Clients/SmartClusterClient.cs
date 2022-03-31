using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var replicaCounter = ReplicaAddresses.Length;
            var averageTimeout = timeout / replicaCounter;
            var pendingRequests = new List<Task<string>>();

            //ReplicaAddresses.OrderBy(x => random.Next()) -- ShouldNotSpendTimeOnBad test failed
            foreach (var uri in ReplicaAddresses)
            {
                var request = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {request.RequestUri}");
                var currentRequest = ProcessRequestAsync(request);

                pendingRequests.Add(currentRequest);
                var timeoutTask = Task.Delay(averageTimeout);
                do
                {
                    var completedTask = await Task.WhenAny(pendingRequests.Append(timeoutTask));
                    if (completedTask is Task<string> completedRequest)
                    {
                        if (completedRequest.IsCompletedSuccessfully)
                            return completedRequest.Result;

                        //bad requests
                        pendingRequests.Remove(completedRequest);
                        averageTimeout = timeout / (--replicaCounter);
                        if (completedRequest == currentRequest)
                            break;
                    }
                } while (!timeoutTask.IsCompleted);
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}