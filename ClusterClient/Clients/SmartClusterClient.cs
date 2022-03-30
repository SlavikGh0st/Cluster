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
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");

                var currentRequest = ProcessRequestAsync(webRequest);
                pendingRequests.Add(currentRequest);
                var delay = Task.Delay(averageTimeout);
                
                do
                {
                    var completedTask = await Task.WhenAny(pendingRequests.Append(delay));
                    if (completedTask is Task<string> request)
                    {
                        if (request.IsCompletedSuccessfully)
                            return request.Result;

                        //bad requests
                        pendingRequests.Remove(request);
                        averageTimeout = timeout / (--replicaCounter);
                        if (request == currentRequest)
                            break;
                    }
                } while (!delay.IsCompleted);
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}