using System;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        private readonly Random random = new Random();

        public RoundRobinClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var replicaCounter = ReplicaAddresses.Length;
            var averageTimeout = timeout / replicaCounter;

            //ReplicaAddresses.OrderBy(x => random.Next()) -- ShouldNotSpendTimeOnBad test failed
            foreach (var uri in ReplicaAddresses)
            {
                var request = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {request.RequestUri}");
                var resultTask = ProcessRequestAsync(request);

                await Task.WhenAny(resultTask, Task.Delay(averageTimeout));
                if (resultTask.IsCompletedSuccessfully)
                    return resultTask.Result;

                if (resultTask.IsFaulted)
                    averageTimeout = timeout / (--replicaCounter);
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}