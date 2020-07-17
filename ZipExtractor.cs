using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AnalyticsProcessor
{
    public static class ZipExtractor
    {
        [FunctionName("ZipExtractor")]
        public static async Task Run([BlobTrigger("analytics-zipped/{name}.zip", Connection = "analyticsStorage")]CloudBlockBlob myBlob, string name, ILogger log)
        {
            log.LogInformation($"Triggering function to extract:{name}");

            string analyticsStorage = Environment.GetEnvironmentVariable("analyticsStorage");
            string destinationContainer = Environment.GetEnvironmentVariable("destinationContainer");

            try{
                if(name.Split('.').Last().ToLower() == "zip"){

                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(analyticsStorage);
                    CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                    CloudBlobContainer container = blobClient.GetContainerReference(destinationContainer);
                    
                    using(MemoryStream blobMemStream = new MemoryStream()){

                        log.LogInformation($"Reading zip file to blob stream");
                        await myBlob.DownloadToStreamAsync(blobMemStream);

                        using(ZipArchive archive = new ZipArchive(blobMemStream))
                        {
                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                log.LogInformation($"Now processing {entry.FullName}");

                                //Replace all NO digits, letters, or "-" by a "-" Azure storage is specific on valid characters
                                string valideName = Regex.Replace(entry.Name,@"[^a-zA-Z0-9\-]","-").ToLower();

                                log.LogInformation($"Writing processed file to unzipped container with name: {valideName}");

                                CloudBlockBlob blockBlob = container.GetBlockBlobReference(valideName);
                                using (var fileStream = entry.Open())
                                {
                                    await blockBlob.UploadFromStreamAsync(fileStream);
                                }
                            }
                        }
                    }
                }
            }
            catch(Exception ex){
                log.LogInformation($"Error! Something went wrong: {ex.Message}");
            }            
        }
    }
}