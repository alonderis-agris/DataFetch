using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace DataFetch
{
    #region Data Contracts
    //{
    //    "count": 1,
    //    "entries": [
    //        {
    //            "API": "CDNJS",
    //            "Description": "Library info on CDNJS",
    //            "Auth": "",
    //            "HTTPS": true,
    //            "Cors": "unknown",
    //            "Link": "https://api.cdnjs.com/libraries/jquery",
    //            "Category": "Development"
    //        }
    //    ]
    //}
    [DataContract(Name = "Entry")]
    public class Entry: TableEntity
    {
        [DataMember]
        public string API
        {
            get;
            set;
        }
        [DataMember]
        public string Description
        {
            get;
            set;
        }
        [DataMember]
        public string Auth
        {
            get;
            set;
        }
        [DataMember]
        public string HTTPS
        {
            get;
            set;
        }
        [DataMember]
        public string Cors
        {
            get;
            set;
        }
        [DataMember]
        public string Link
        {
            get;
            set;
        }
        [DataMember]
        public string Category
        {
            get;
            set;
        }

        public void setAzureFields(int intNpk, int intUnixTS)
        {
            PartitionKey = intUnixTS.ToString();
            RowKey = intUnixTS.ToString() + intNpk.ToString();
        }
    }

    [DataContract(Name = "DataOfEntries")]
    public class DataOfEntries
    {
        [DataMember]
        public int count
        {
            get;
            set;
        }
        [DataMember]
        public Collection<Entry> entries;
    }
    #endregion

    public class DataFetch
    {
        private static HttpClient httpClient = new HttpClient();
        private static string dataUrl = "https://api.publicapis.org/random";
        private static string strDataTableName = "Entry";

        public static CloudStorageAccount getSaCStorageAccount(string scsStorageConnectionString)
        {
            CloudStorageAccount saCStorageAccount_;
            try
            {
                saCStorageAccount_ = CloudStorageAccount.Parse(scsStorageConnectionString);
            }
            catch (FormatException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in config file - then restart the application.");
                throw;
            }
            catch (ArgumentException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in config file - then restart the sample.");
                throw;
            }
            return saCStorageAccount_;
        }
        private static CloudStorageAccount saCStorageAccount = DataFetch.getSaCStorageAccount("UseDevelopmentStorage=true");

        public static async Task<CloudTable> GetCTEntityTask()
        {
            CloudTableClient tcTableClient = DataFetch.saCStorageAccount.CreateCloudTableClient();
            CloudTable ctEntity_ = tcTableClient.GetTableReference(DataFetch.strDataTableName);

            try
            {
                if (await ctEntity_.CreateIfNotExistsAsync())
                {
                    Console.WriteLine("Created Table named: {0}", DataFetch.strDataTableName);
                }
                else
                {
                    Console.WriteLine("Table {0} already exists", DataFetch.strDataTableName);
                }
            }
            catch (StorageException)
            {
                Console.WriteLine("If you are running with the default configuration please make sure you have started the storage emulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                Console.ReadLine();
                throw;
            }
            return ctEntity_;
        }
        public static Task<CloudTable> tctTask = GetCTEntityTask();
        public static CloudTable GetCTEntity()
        {
            DataFetch.tctTask.Wait();
            return DataFetch.tctTask.Result;
        }
        private static CloudTable ctEntity = DataFetch.GetCTEntity();

        private static string getFullDataUrl(Dictionary<string, string> dictionary)
        {
            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<string, string> kvpItem in dictionary)
            {
                if (sb.Length == 0)
                    sb.Append(DataFetch.dataUrl + "?");
                else
                    sb.Append("&");
                sb.Append(kvpItem.Key + "=" + kvpItem.Value);
            }
            return sb.ToString();
        }
        private static async Task<string> getData()
        {
            Dictionary<string, string> dictionary = new Dictionary<string, string>();
            dictionary.Add("auth", "null");
            var strRet = await DataFetch.httpClient.GetStringAsync(getFullDataUrl(dictionary));
            return strRet;
        }
        private static async Task<int> SaveDataAsync(DataOfEntries doeData, CloudTable ctTable)
        {
            if (doeData.count == 0)
                return 0;
            Int32 currUnixTimestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
            for (int i = 0; i < doeData.entries.Count; i++)
            {
                try
                {
                    // Create the InsertOrReplace table operation
                    doeData.entries[i].setAzureFields(i, currUnixTimestamp);
                    TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge(doeData.entries[i]);
                    // Execute the operation
                    TableResult result = await ctTable.ExecuteAsync(insertOrMergeOperation);
                }
                catch (StorageException e)
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                    return -1;
                }
            }
            return 1;
        }
        public static async Task<DataOfEntries> GetDataAsyncInRange(int intFromUT, int intToUT)
        {
            DataOfEntries doeRet = new DataOfEntries();
            doeRet.count = 0;

            TableQuery<Entry> entryQuery = new TableQuery<Entry>().Where(
            TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, intFromUT.ToString()),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, intToUT.ToString())
                )
            );
            TableContinuationToken tctContinuationToken = null;
            do
            {
                var result = await ctEntity.ExecuteQuerySegmentedAsync(entryQuery, tctContinuationToken);
                tctContinuationToken = result.ContinuationToken;
                if (result.Results != null)
                    foreach (Entry entity in result.Results)
                    {
                        doeRet.count++;
                        doeRet.entries.Add(entity);
                    }

            } while (tctContinuationToken != null);
            return doeRet;
        }

        public static async Task<Entry> getSingleRecord(int intId)
        {
            Entry eRet = new Entry();
            string strId = intId.ToString();
            string strPartitionId = strId.Substring(1, 10);
            TableOperation toGetSingleRecord = TableOperation.Retrieve<ITableEntity>(strPartitionId, strId);
            TableResult query = await ctEntity.ExecuteAsync(toGetSingleRecord);
            if (query.Result != null)
                eRet = (Entry)query.Result;
            return eRet;
        }

        [FunctionName("DataFetch")]
        public static async void Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer, ILogger log)
        {
            //Task<string> tsGetData = getData();
            //tsGetData.Wait();
            //var strData = tsGetData.Result;
            string strData =
            "{\"count\":1,\"entries\":[{\"API\":\"Russian Calendar\",\"Description\":\"Check if a date is a Russian holiday or not\",\"Auth\":\"\",\"HTTPS\":true,\"Cors\":\"no\",\"Link\":\"links\",\"Category\":\"Calendar\"}]}";
            DataContractJsonSerializer dcjs = new DataContractJsonSerializer(typeof(DataOfEntries));
            MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(strData.ToString()));
            ms.Position = 0;
            DataOfEntries doeData = (DataOfEntries)dcjs.ReadObject(ms);
            Task<int> sdaTask = SaveDataAsync(doeData, DataFetch.ctEntity);
            sdaTask.Wait();
            int res = sdaTask.Result;
        }
    }
}
