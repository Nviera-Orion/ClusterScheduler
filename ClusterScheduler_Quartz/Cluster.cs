using Microsoft.Azure.Management.ResourceManager;
using Microsoft.Azure.Management.ResourceManager.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClusterScheduler_Quartz
{
    /// <summary>
    /// 1. Create the Resource Group
    /// 2. Create the Hdinsight Cluster
    /// 3. Submit the Pig jobs to the Cluster
    /// 4. Submit the Spark jobs to the Cluster
    /// 5. Delete the Resource group
    /// 6. Delete the 
    /// </summary>
    public static class Cluster
    {
        public static string Status { get; private set; }
        static string groupName = "ExampleResourceGroup";
        static string storageName = "clustercreatetemplate";
        static string location = "East US";
        static string subscriptionId = "e4337d44-53e5-48eb-b1ba-6652656b470e";
        static string deploymentName = "orionml";

        public static async Task<bool> Create()
        {
            var token = SetAuthTokenAsync();
            var credential = new TokenCredentials(token.Result.AccessToken);
            var rgResult = CreateResourceGroupAsync(credential, groupName, subscriptionId, location);
            if(rgResult.Result.Properties.ProvisioningState == "sucess")
            {
                var dpResult = CreateTemplateDeploymentAsync(credential, groupName, storageName, deploymentName, subscriptionId);
                await Console.Out.WriteLineAsync("*********************************");
                await Console.Out.WriteLineAsync("Cluster Creation in Process");
               
                if (dpResult.Result.Properties.ProvisioningState == "sucess")
                {
                    Status = "ON";
                    return true;
                }
                 else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }            
        }

        public static void Delete()
        {
            var token = SetAuthTokenAsync();
            var credential = new TokenCredentials(token.Result.AccessToken);
            DeleteResourceGroupAsync(credential, groupName, subscriptionId);
            Status = "OFF";
        }

        
        private static async Task<AuthenticationResult> SetAuthTokenAsync()
        {
            var clientid = "055c54b8-5b6f-4bc9-8a59-028d3f70f9b6";
            var clientsecret = "B4oB9hzq/ADLC5vxxcI95DKDdlt98WxtTgSVmlsZgyk=";
            var cc = new ClientCredential(clientid, clientsecret);
            var context = new AuthenticationContext("https://login.windows.net/15b79de4-baff-453e-8df8-6865beac9b8c");
            var token =  await context.AcquireTokenAsync("https://management.azure.com/", cc);
            if (token == null)
            {
                throw new InvalidOperationException("Could not get the token.");
            }
            return token;
        }

        private static async Task<ResourceGroup> CreateResourceGroupAsync(TokenCredentials credential, string groupName, string subscriptionId, string location)
        {
            Console.WriteLine("Creating the resource group...");
            var resourceManagementClient = new ResourceManagementClient(credential)
            { SubscriptionId = subscriptionId };
            var resourceGroup = new ResourceGroup { Location = location };
            return await resourceManagementClient.ResourceGroups.CreateOrUpdateAsync(groupName, resourceGroup);
        }

        private static async Task<DeploymentExtended> CreateTemplateDeploymentAsync(TokenCredentials credential, string groupName, string storageName,string deploymentName, string subscriptionId)
        {
            Console.WriteLine("Creating the template deployment..."+ DateTime.Now);
            var deployment = new Deployment();
            deployment.Properties = new DeploymentProperties
            {
                Mode = DeploymentMode.Incremental,
                TemplateLink = new TemplateLink
                {
                    //https://clustercreatetemplate.blob.core.windows.net/resourcetemplate/template.json
                    //https://clustercreatetemplate.blob.core.windows.net/sparkdheetest/user/root/ARMCluster.json --LATEST
                    Uri = "https://" + storageName + ".blob.core.windows.net/sparkdheetest/user/root/ARMCluster.json"

                },
                ParametersLink = new ParametersLink
                {
                    //https://clustercreatetemplate.blob.core.windows.net/resourcetemplate/parameters.json
                    //https://clustercreatetemplate.blob.core.windows.net/sparkdheetest/user/root/ARMParameters.json -LATEST
                    Uri = "https://" + storageName + ".blob.core.windows.net/sparkdheetest/user/root/ARMParameters.json"
                  
                }
            };
            var resourceManagementClient = new ResourceManagementClient(credential)
            { SubscriptionId = subscriptionId };
            return await resourceManagementClient.Deployments.CreateOrUpdateAsync(groupName, deploymentName, deployment);
        }

        public static async void DeleteResourceGroupAsync(TokenCredentials credential, string groupName, string subscriptionId)
        {
            Console.WriteLine("Deleting resource group...");
            var resourceManagementClient = new ResourceManagementClient(credential)
            { SubscriptionId = subscriptionId };
            await resourceManagementClient.ResourceGroups.DeleteAsync(groupName);
        }

    }
}
