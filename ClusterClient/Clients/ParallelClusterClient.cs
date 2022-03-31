using System;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class ParallelClusterClient : ClusterClientBase
    {
        public ParallelClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var pendingRequests = ReplicaAddresses
                .Select(async uri =>
                {
                    var request = CreateRequest(uri + "?query=" + query);
                    Log.InfoFormat($"Processing {request.RequestUri}");
                    var resultTask = ProcessRequestAsync(request);

                    await Task.WhenAny(resultTask, Task.Delay(timeout));
                    if (!resultTask.IsCompleted)
                        throw new TimeoutException();

                    return resultTask.Result;
                }).ToList();

            while (pendingRequests.Count > 0)
            {
                var completedTask = await Task.WhenAny(pendingRequests);
                if (completedTask.IsCompletedSuccessfully)
                    return completedTask.Result;
                pendingRequests.Remove(completedTask);
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
    }
}