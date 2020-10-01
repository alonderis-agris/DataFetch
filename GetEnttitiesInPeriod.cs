using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System.Runtime.Serialization.Json;

namespace DataFetch
{
    public static class GetEnttitiesInPeriod
    {
        private static int intUnixTimeStampFrom;
        private static int intUnixTimeStampTo;
        private static CloudStorageAccount saCStorageAccount = DataFetch.getSaCStorageAccount("UseDevelopmentStorage=true");
        private static CloudTable ctEntity = DataFetch.GetCTEntity();

        [FunctionName("GetEnttitiesInPeriod")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            #region Set query variables
            string strFrom = req.Query["from"];
            string strTo = req.Query["to"];

            intUnixTimeStampFrom = 0;
            intUnixTimeStampTo = 0;

            try {
                intUnixTimeStampFrom = int.Parse(strFrom);
            }
            catch {
                intUnixTimeStampFrom = 0;
            }
            try {
                intUnixTimeStampTo = int.Parse(strTo);
            }
            catch
            {
                intUnixTimeStampTo = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
            }
            #endregion

            #region Query dataBase
            DataFetch dfDataFetch = new DataFetch();
            Task<DataOfEntries> tdoe = DataFetch.GetDataAsyncInRange(intUnixTimeStampFrom, intUnixTimeStampTo);
            tdoe.Wait();
            DataOfEntries doeResults = (DataOfEntries)tdoe.Result;
            #endregion

            DataContractJsonSerializer js = new DataContractJsonSerializer(typeof(DataOfEntries));
            MemoryStream msObj = new MemoryStream();
            js.WriteObject(msObj, doeResults);
            msObj.Position = 0;
            StreamReader srStreamReader = new StreamReader(msObj);
            string strJson = srStreamReader.ReadToEnd();
            srStreamReader.Close();
            msObj.Close();

            string responseMessage = !(string.IsNullOrEmpty(strFrom) && string.IsNullOrEmpty(strTo))
                ? strJson
                : "Missing query params!";

            return new OkObjectResult(responseMessage);
        }
    }
}
