using log4net;
using RestSharp;
using RestSharp.Authenticators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClusterScheduler_Quartz
{
    public static class SparkSubmit
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public static void PostSparkJob()
        {
            try
            {
                var client = new RestClient("https://orionml.azurehdinsight.net/livy/");
                client.Authenticator = new HttpBasicAuthenticator("admin", "Raptor1971#s");

                var request = new RestRequest("batches", Method.POST);
                var batchBody = request.JsonSerializer.Serialize(new BatchRequest { className = "com.orionml.spark.outlier", file = "wasbs:///user/root/orionml-assembly-7.0.jar" });
                request.AddParameter("application/json; charset=utf-8", batchBody, ParameterType.RequestBody);

                var res = client.Execute<BatchResponse>(request);
                var createResp = res.Data;

            }
            catch(Exception ex)
            {
                log.Debug("Oops !!! Something broke in SPark Submit " + ex.Message);
            }
            
        }
    }

    public class BatchResponse
    {
        public string id { get; set; }
        public string state { get; set; }
        public string log { get; set; }
    }

    public class BatchRequest
    {
        public string file { get; set; }
        public string className { get; set; }
      

    }
}
