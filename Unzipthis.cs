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

namespace AzUnzipEverything
{
    public static class Unzipthis
    {
        [FunctionName("Unzipthis")]
        public static async Task Run([BlobTrigger("input-files/{name}", Connection = "cloud5mins_storage")]CloudBlockBlob myBlob, string name, ILogger log)
        {
            log.LogInformation($"Triggering function to extract:{name}");

            string destinationStorage = Environment.GetEnvironmentVariable("destinationStorage");
            string destinationContainer = Environment.GetEnvironmentVariable("destinationContainer");

            try{
                if(name.Split('.').Last().ToLower() == "zip"){

                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(destinationStorage);
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

                                log.LogInformation($"Original name: {entry.Name}");
                                log.LogInformation($"Writing processed file to unzipped container with name: {valideName}");

                                /*CloudBlockBlob blockBlob = container.GetBlockBlobReference(valideName);
                                using (var fileStream = entry.Open())
                                {
                                    await blockBlob.UploadFromStreamAsync(fileStream);
                                }*/
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