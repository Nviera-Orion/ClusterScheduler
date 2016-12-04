using log4net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClusterScheduler_Quartz
{
    public class PollingJob : IJob
    {
        public readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public async Task Execute(IJobExecutionContext context)
        {
            string inputBlob = "";
            await Console.Out.WriteLineAsync("Polling the Container");
            await Console.Out.WriteLineAsync("CLUSTER STATUS "+ Cluster.Status);

            var blobPollResult = await PollContainerAsync(inputBlob);
            //
            if (blobPollResult)
            {
                //Call the Cluster Creation Code
                await Console.Out.WriteLineAsync("===================================");
                await Console.Out.WriteLineAsync("Gzip File Detected" + DateTime.Now);
                await Console.Out.WriteLineAsync("Starting the Cluster Creation Process");                
                await Console.Out.WriteLineAsync("Scheduler on Standy Mode");

                JobScheduler.Pause();

                if (Cluster.Status == "OFF")
                {
                    Task<bool> createClusterResult = Cluster.Create();
                    try
                    {                       
                        var result = await createClusterResult;
                        if (result)
                        {
                            await Console.Out.WriteLineAsync("Cluster Created");
                            await Console.Out.WriteLineAsync("===================================");
                            await Console.Out.WriteLineAsync("Scheduler Poll Mode ON");

                            JobScheduler.Resume();
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.Out.WriteLine("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
                        log.Debug("Oops!!!Something broke While creating the Cluster--"+ex.InnerException);
                        Console.Out.WriteLine("Oops!!! Something broke While creating the Cluster --" + ex.Message);
                    }

                    Console.Out.WriteLine("Create Cluster Task IsFaulted:  " + createClusterResult.IsFaulted);
                    if (createClusterResult.Exception != null)
                    {
                        Console.Out.WriteLine("Task Exception Message: " + createClusterResult.Exception.Message);
                        Console.Out.WriteLine("Task Inner Exception Message: " + createClusterResult.Exception.InnerException.Message);
                        Console.Out.WriteLine("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
                    }

                }

            }
        }

        /// <summary>
        /// Polls the Customer Container maintained specifically for input Gzip. 
        /// Returns the exsistence of input blob from the container
        /// </summary>
        /// <returns></returns>
        public bool PollContainer()
        {
            try
            {
                var storageConnstring = "DefaultEndpointsProtocol=https;AccountName=clustercreatetemplate;AccountKey=Doo5F7Bw9gPdFDAdJtq9UMeuh9VZ+3C4TPEK8I2ZwR9Dud3fw6jJkoLMhWF26+2l5gb8iTxc4K+Fr0YSFPNK2w==";

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnstring);

                // Create the blob client.
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();


                // Retrieve a reference to a container.
                var containername = "fromelk";
                CloudBlobContainer container = blobClient.GetContainerReference(containername);

                // Retrieve reference to a blob named "myblob.txt"
                CloudBlockBlob blockBlobRef = container.GetBlockBlobReference("Proxyfinal4.gz");

                return blockBlobRef.Exists();
            }
            catch (Exception ex)
            {
                log.Debug("Oops ! Something broke in PollContainer method : -->" + ex.InnerException);
            }
            return false;


        }

        public async Task<bool> PollContainerAsync(string inputBlob)
        {
            try
            {
                var storageConnstring = "DefaultEndpointsProtocol=https;AccountName=clustercreatetemplate;AccountKey=Doo5F7Bw9gPdFDAdJtq9UMeuh9VZ+3C4TPEK8I2ZwR9Dud3fw6jJkoLMhWF26+2l5gb8iTxc4K+Fr0YSFPNK2w==";

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnstring);

                // Create the blob client.
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();


                // Retrieve a reference to a container.
                var containername = "fromelk";
                CloudBlobContainer container = blobClient.GetContainerReference(containername);

                // Retrieve reference to a blob named "myblob.txt"
                CloudBlockBlob blockBlobRef =  container.GetBlockBlobReference(inputBlob);

                return await blockBlobRef.ExistsAsync();
            }
            catch (Exception ex)
            {
                log.Debug("Oops ! Something broke in PollContainer method : -->" + ex.InnerException);
            }
            return false;


        }
    }
}
