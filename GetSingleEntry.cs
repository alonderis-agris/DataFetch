using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Runtime.Serialization.Json;

namespace DataFetch
{
    public static class GetSingleEntry
    {
        [FunctionName("GetSingleEntry")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string strId = req.Query["id"];
            int intId = -1;
            try { 
                intId = int.Parse(strId); 
            }
            catch { 
                intId = 1; 
            }

            #region Query dataBase
            DataFetch dfDataFetch = new DataFetch();
            Task<Entry> te = DataFetch.getSingleRecord(intId);
            te.Wait();
            DataOfEntries doeResults = new DataOfEntries();
            Entry e = (Entry) te.Result;
            if (string.IsNullOrEmpty(e.PartitionKey))
                doeResults.count = 0;
            else
            {
                doeResults.count = 1;
                doeResults.entries.Add(e);
            }
            #endregion

            #region Generate result string
            DataContractJsonSerializer js = new DataContractJsonSerializer(typeof(DataOfEntries));
            MemoryStream msObj = new MemoryStream();
            js.WriteObject(msObj, doeResults);
            msObj.Position = 0;
            StreamReader srStreamReader = new StreamReader(msObj);
            string strJson = srStreamReader.ReadToEnd();
            srStreamReader.Close();
            msObj.Close();
            #endregion

            string strResponseMessage = null;
            if (intId == -1)
                strResponseMessage = "Query parameter not in valid format!";
            else
                strResponseMessage = strJson;

            return new OkObjectResult(strResponseMessage);
        }
    }
}
