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
            var webRequests = ReplicaAddresses.Select(async uri =>
            {
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");
                var resultTask = ProcessRequestAsync(webRequest);

                await Task.WhenAny(resultTask, Task.Delay(timeout));
                if (!resultTask.IsCompleted)
                    throw new TimeoutException();

                return resultTask.Result;
            }).ToList();

            while (webRequests.Count > 0)
            {
                await Task.WhenAny(webRequests);

                var completed = webRequests.First(request => request.IsCompleted);
                if (completed.IsCompletedSuccessfully)
                    return completed.Result;
                webRequests.Remove(completed);
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
    }
}

