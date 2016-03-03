// This code requires the Nuget package Microsoft.AspNet.WebApi.Client to be installed.
// Instructions for doing this in Visual Studio:
// Tools -> Nuget Package Manager -> Package Manager Console
// Install-Package Microsoft.AspNet.WebApi.Client
//
// Also, add a reference to Microsoft.WindowsAzure.Storage.dll for reading from and writing to the Azure blob storage

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace CallBatchExecutionService
{
    public class AzureBlobDataReference
    {
        public string ConnectionString { get; set; }
        public string RelativeLocation { get; set; }
        public string BaseLocation { get; set; }
        public string SasBlobToken { get; set; }
    }

    public enum BatchScoreStatusCode
    {
        NotStarted,
        Running,
        Failed,
        Cancelled,
        Finished
    }

    public class BatchScoreStatus
    {
        // Status code for the batch scoring job
        public BatchScoreStatusCode StatusCode { get; set; }

        // Locations for the potential multiple batch scoring outputs
        public IDictionary<string, AzureBlobDataReference> Results { get; set; }

        // Error details, if any
        public string Details { get; set; }
    }

    public class BatchExecutionRequest
    {
        public IDictionary<string, AzureBlobDataReference> Inputs { get; set; }
        public IDictionary<string, string> GlobalParameters { get; set; }
        public IDictionary<string, AzureBlobDataReference> Outputs { get; set; }
    }

    class Program
    {
        private static Dictionary<string, string> _configuration;

        static void Main(string[] args)
        {
            loadConfig();

            InvokeBatchExecutionService().Wait();
        }

        static private void loadConfig()
        {
            using (StreamReader file = File.OpenText("C:/tmp/configuration.json"))
            {
                JsonSerializer serializer = new JsonSerializer();
                _configuration = (Dictionary<string, string>)serializer.Deserialize(file, typeof(Dictionary<string, string>));
            }
        }

        static async Task WriteFailedResponse(HttpResponseMessage response)
        {
            Console.WriteLine($"The request failed with status code: {response.StatusCode}");

            // Print the headers - they include the request ID and the timestamp, which are useful for debugging the failure
            Console.WriteLine(response.Headers.ToString());

            string responseContent = await response.Content.ReadAsStringAsync();
            Console.WriteLine(responseContent);
        }

        static void UploadFileToBlob(string inputFileLocation, string inputBlobName, string storageContainerName, string storageConnectionString)
        {
            // Make sure the file exists
            if (!File.Exists(inputFileLocation))
            {
                throw new FileNotFoundException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "File {0} doesn't exist on local computer.",
                        inputFileLocation));
            }

            Console.WriteLine("Uploading the input to blob storage...");

            var blobClient = CloudStorageAccount.Parse(storageConnectionString).CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(storageContainerName);
            container.CreateIfNotExists();
            var blob = container.GetBlockBlobReference(inputBlobName);
            blob.UploadFromFile(inputFileLocation, FileMode.Open);
        }

        static void ProcessResults(BatchScoreStatus status)
        {
            foreach (var output in status.Results)
            {
                var blobLocation = output.Value;
                Console.WriteLine($"The result '{output.Key}' is available at the following Azure Storage location");
                Console.WriteLine($@"Download the new model's evaluation:\n
                    {
                        blobLocation.BaseLocation +
                        blobLocation.RelativeLocation +
                        blobLocation.SasBlobToken
                    }
                ");
            }
        }

        static async Task InvokeBatchExecutionService()
        {
            // How this works:
            //
            // 1. Assume the input is present in a local file (if the web service accepts input)
            // 2. Upload the file to an Azure blob - you'd need an Azure storage account
            // 3. Call the Batch Execution Service to process the data in the blob. Any output is written to Azure blobs.
            // 4. Download the output blob, if any, to local file

            string StorageAccountName = _configuration["storageacc_name"];
            string StorageAccountKey = _configuration["storageacc_key"];
            string StorageContainerName = _configuration["storageacc_containername"];

            // set a time out for polling status
            const int TimeOutInMilliseconds = 120 * 1000; // Set a timeout of 2 minutes

            string storageConnectionString = $"DefaultEndpointsProtocol=https;AccountName={StorageAccountName};AccountKey={StorageAccountKey}";

            UploadFileToBlob(_configuration["localfilepath"],
               "input1datablob.csv",
               StorageContainerName, storageConnectionString);

            using (HttpClient client = new HttpClient())
            {
                var request = new BatchExecutionRequest()
                {
                    Inputs = new Dictionary<string, AzureBlobDataReference>()
                    {
                        {
                            "input1",
                            new AzureBlobDataReference()
                            {
                                ConnectionString = storageConnectionString,
                                RelativeLocation = $"{StorageContainerName}/input1datablob.csv"
                            }
                        },
                    },

                    Outputs = new Dictionary<string, AzureBlobDataReference>()
                    {
                        {
                            "output1",
                            new AzureBlobDataReference()
                            {
                                ConnectionString = storageConnectionString,
                                RelativeLocation = $"/{StorageContainerName}/output1results.csv"
                            }
                        },
                        {
                            "output2",
                            new AzureBlobDataReference()
                            {
                                ConnectionString = storageConnectionString,
                                RelativeLocation = $"/{StorageContainerName}/output2results.ilearner"
                            }
                        },
                    },
                    GlobalParameters = new Dictionary<string, string>()
                    {
                    }
                };

                string BaseUrl = _configuration["training_endpoint_uri"];
                string apiKey = _configuration["training_endpoint_key"];

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

                // WARNING: The 'await' statement below can result in a deadlock if you are calling this code from the UI thread of an ASP.Net application.
                // One way to address this would be to call ConfigureAwait(false) so that the execution does not attempt to resume on the original context.
                // For instance, replace code such as:
                //      result = await DoSomeTask()
                // with the following:
                //      result = await DoSomeTask().ConfigureAwait(false)

                Console.WriteLine("Submitting the job...");

                // submit the job
                var response = await client.PostAsJsonAsync(BaseUrl + "?api-version=2.0", request);
                if (!response.IsSuccessStatusCode)
                {
                    await WriteFailedResponse(response);
                    return;
                }

                string jobId = await response.Content.ReadAsAsync<string>();
                Console.WriteLine($"Job ID: {jobId}");

                // start the job
                Console.WriteLine("Starting the job...");
                response = await client.PostAsync(BaseUrl + "/" + jobId + "/start?api-version=2.0", null);
                if (!response.IsSuccessStatusCode)
                {
                    await WriteFailedResponse(response);
                    return;
                }

                string jobLocation = BaseUrl + "/" + jobId + "?api-version=2.0";
                Stopwatch watch = Stopwatch.StartNew();
                bool done = false;
                while (!done)
                {
                    Console.WriteLine("Checking the job status...");
                    response = await client.GetAsync(jobLocation);
                    if (!response.IsSuccessStatusCode)
                    {
                        await WriteFailedResponse(response);
                        return;
                    }

                    BatchScoreStatus status = await response.Content.ReadAsAsync<BatchScoreStatus>();
                    if (watch.ElapsedMilliseconds > TimeOutInMilliseconds)
                    {
                        done = true;
                        Console.WriteLine($"Timed out. Deleting job {jobId} ...");
                        await client.DeleteAsync(jobLocation);
                    }
                    switch (status.StatusCode)
                    {
                        case BatchScoreStatusCode.NotStarted:
                            Console.WriteLine($"Job {jobId} not yet started...");
                            break;
                        case BatchScoreStatusCode.Running:
                            Console.WriteLine($"Job {jobId} running...");
                            break;
                        case BatchScoreStatusCode.Failed:
                            Console.WriteLine($"Job {jobId} failed!");
                            Console.WriteLine($"Error details: {status.Details}");
                            done = true;
                            break;
                        case BatchScoreStatusCode.Cancelled:
                            Console.WriteLine($"Job {jobId} cancelled!");
                            done = true;
                            break;
                        case BatchScoreStatusCode.Finished:
                            done = true;
                            Console.WriteLine($"Job {jobId} finished!");

                            ProcessResults(status);
                            Console.WriteLine("Overwrite existing model? <y/n>");
                            var res = Console.ReadLine().ToLower();
                            if (res == "y")
                                await OverwriteModel(status);
                            break;
                    }

                    if (!done)
                    {
                        Thread.Sleep(1000); // Wait one second
                    }
                }
            }
        }

        private static async Task OverwriteModel(BatchScoreStatus status)
        {
            // status contains 2 outputs - we want the output with key Output2 which is at index 0
            var newModelBlobLocation = status.Results.ElementAt(0).Value;
            string apiKey = _configuration["trainable_endpoint_key"];
            string endpointURL = _configuration["trainable_endpoint_uri"];

            var resourceLocations = new
            {
                Resources = new[]
                {
                    new
                    {
                        Name = "Census Model [trained model]",
                        Location = new AzureBlobDataReference()
                        {
                            BaseLocation = newModelBlobLocation.BaseLocation,
                            RelativeLocation = newModelBlobLocation.RelativeLocation,
                            SasBlobToken = newModelBlobLocation.SasBlobToken
                        }
                    }
                }
            };

            using (var client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

                using (var request = new HttpRequestMessage(new HttpMethod("PATCH"), endpointURL))
                {
                    request.Content = new StringContent(JsonConvert.SerializeObject(resourceLocations), System.Text.Encoding.UTF8, "application/json");
                    HttpResponseMessage response = await client.SendAsync(request);

                    if (!response.IsSuccessStatusCode)
                    {
                        await WriteFailedResponse(response);
                    }

                    // Do what you want with a successful response here.
                    Console.WriteLine("Successfully updated the retraining endpoint model - Please wait 15 minutes for the service to fully migrate");
                    Console.WriteLine("Press any key to finish");
                    Console.ReadLine();
                }
            }
        }
    }
}

