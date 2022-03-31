using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public abstract class SmartClusterClientBase
    {
        protected Dictionary<string, TimeSpan> ReplicaAddresses { get; set; }

        protected SmartClusterClientBase(IEnumerable<string> replicaAddresses)
        {
            ReplicaAddresses = new Dictionary<string, TimeSpan>(
                replicaAddresses.Select(uri =>
                    new KeyValuePair<string, TimeSpan>(uri, TimeSpan.MaxValue)));
        }

        public abstract Task<string> ProcessRequestAsync(string query, TimeSpan timeout);
        protected abstract ILog Log { get; }

        protected static HttpWebRequest CreateRequest(string uriStr)
        {
            var request = WebRequest.CreateHttp(Uri.EscapeUriString(uriStr));
            request.Proxy = null;
            request.KeepAlive = true;
            request.ServicePoint.UseNagleAlgorithm = false;
            request.ServicePoint.ConnectionLimit = 100500;
            return request;
        }

        protected async Task<string> ProcessRequestAsync(WebRequest request, string uri)
        {
            var timer = Stopwatch.StartNew();
            using (var response = await request.GetResponseAsync())
            {
                var result = await new StreamReader(response.GetResponseStream(), Encoding.UTF8).ReadToEndAsync();
                Log.InfoFormat("Response from {0} received in {1} ms", request.RequestUri, timer.ElapsedMilliseconds);
                ReplicaAddresses[uri] = timer.Elapsed;
                return result;
            }
        }
    }
}