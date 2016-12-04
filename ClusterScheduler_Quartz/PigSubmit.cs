using Hyak.Common;
using log4net;
using Microsoft.Azure.Management.HDInsight.Job;
using Microsoft.Azure.Management.HDInsight.Job.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClusterScheduler_Quartz
{
    public static class PigSubmit
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public static void RunJob()
        {
            List<string> filenames = new List<string>();

            foreach(var filename in filenames)
            {
                try
                {
                    SubmitPigJob(filename);
                }
                catch (Exception ex)
                {
                    log.Debug("Oops!!! Something broke while submitting the Pig job with file name [" + filename + "]" + ex.Message);

                }
            }
        }

        public static void SubmitPigJob(string filename)
        {
            const string ExistingClusterName = "orionml";
            const string ExistingClusterUri = ExistingClusterName + ".azurehdinsight.net";
            const string ExistingClusterUsername = "admin";
            const string ExistingClusterPassword = "Raptor1971#s";

            var clusterCredentials = new BasicAuthenticationCloudCredentials { Username = ExistingClusterUsername, Password = ExistingClusterPassword };
            HDInsightJobManagementClient _hdiJobManagementClient = new HDInsightJobManagementClient(ExistingClusterUri, clusterCredentials);


            var parameters = new PigJobSubmissionParameters
            {                
                File = "wasbs:///user/root/"+ filename + ".pig"               
            };

            Console.WriteLine("Submitting the Pig job with file name ["+ filename+"]  to the cluster...");
            var response = _hdiJobManagementClient.JobManagement.SubmitPigJob(parameters);          
            Console.WriteLine("JobId is " + response.JobSubmissionJsonResponse.Id);

                   
        }
    }
}
