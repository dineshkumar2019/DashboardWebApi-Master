using System;
using System.Linq;
using System.Net;
using System.Net.Http;  
using System.Web.Http;
using System.Data;
using System.Configuration;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Web;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Web.Http.Cors;

namespace DashboardWebApi.Controllers
{
    [EnableCors(origins: "*", headers:"*",methods:"*")]
    [RoutePrefix("api/values")]
    public class ValuesController : ApiController
    {
        string connStr = ConfigurationManager.ConnectionStrings["ConnStringSQL"].ConnectionString;

        /******* Catalog Module *****/
        //Get database table preview rows based on selected tablename
        [HttpGet]
        [Route("TablePreview/{selTableName}/{selSchemaName}")]
        public HttpResponseMessage GetTablePreview(string selTableName, string selSchemaName)
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "SELECT TOP (20) * FROM [" + selSchemaName + "].[" + selTableName + "]";
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get database table schema based on selected tablename
        [HttpGet]
        [Route("TableSchema/{selTableName}/{selSchemaName}")]
        public HttpResponseMessage GetTableSchema(String selTableName, string selSchemaName)
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);

                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "SELECT column_name, column_default, is_Nullable, data_type, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS where table_name in (select name from sys.tables where type ='U' and table_schema = '" + selSchemaName + "' and name = '" + selTableName + "')";

                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);            
            }
        }

        //Get database table profile details - based on filename. Get the xml file from azure blob storage and then extract the profile details
        [HttpGet]
        [Route("TableProfile/{selTableName}")]
        public HttpResponseMessage GetTableProfile(String selTableName)
        {
            try
            {
                HttpResponseMessage response;
                var containerName = Convert.ToString(ConfigurationManager.AppSettings["ProfileContainer"]);       
                string selXmlFile = Convert.ToString(ConfigurationManager.AppSettings["ProfileFilePrefix"]) + selTableName + Convert.ToString(ConfigurationManager.AppSettings["ProfileFileExtn"]);

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(containerName);
                CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(selXmlFile);
                if (!blockBlob.Exists())
                {
                    return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Sorry, the file "+ selXmlFile +" does not exist in Azure");                    
                }
                MemoryStream memStream = new MemoryStream();
                blockBlob.DownloadToStream(memStream);

                memStream.Position = 0;
                DataSet ds = new DataSet();
                ds.ReadXml(memStream);

                DataTable dtColumn = ds.Tables["Column"];
                DataTable dtColumnNullRatioProfile = ds.Tables["ColumnNullRatioProfile"];
                DataTable dtColumnValueDistributionProfile = ds.Tables["ColumnValueDistributionProfile"];
                DataTable dtColumnStatisticsProfile = ds.Tables["ColumnStatisticsProfile"];

                // From Column Datatable get the Column Name   
                var drColumnName = from col in dtColumn.AsEnumerable()
                                   where col["Name"].ToString() != ""
                                   select new
                                   {
                                       ColumnName = col.Field<string>("Name")
                                   };

                //From Column Datatable get the distinct Column Name
                var drColumnNameDistinct = drColumnName.GroupBy(x => x.ColumnName).Select(x => x.FirstOrDefault());

                //From Column Datatable get the ColumnStatisticsProfile_Id along with the Column Name
                var drColumnStatProfIds = from col in dtColumn.AsEnumerable()
                                          where col["ColumnStatisticsProfile_Id"].ToString() != ""
                                          select new
                                          {
                                              ColumnStatisticsProfile_Id = col.Field<Int32>("ColumnStatisticsProfile_Id").ToString(),
                                              ColumnName = col.Field<string>("Name")
                                          };

                //From Column Datatable get the ColumnNullRatioProfile_Id along with the Column Name
                var drColumnNullProfIds = from col in dtColumn.AsEnumerable()
                                          where col["ColumnNullRatioProfile_Id"].ToString() != ""
                                          select new
                                          {
                                              ColumnNullRatioProfile_Id = col.Field<Int32>("ColumnNullRatioProfile_Id").ToString(),
                                              ColumnName = col.Field<string>("Name")
                                          };

                //From Column Datatable get the ColumnValueDistributionProfile_Id along with the Column Name 
                var drColumnDistProfIds = from col in dtColumn.AsEnumerable()
                                          where col["ColumnValueDistributionProfile_Id"].ToString() != ""
                                          select new
                                          {
                                              ColumnValueDistributionProfile_Id = col.Field<Int32>("ColumnValueDistributionProfile_Id").ToString(),
                                              ColumnName = col.Field<string>("Name")
                                          };

                //****Start - Building query to fetch result set with Column Name, ColumnStatisticsProfile_Id, ColumnNullRatioProfile_Id, ColumnValueDistributionProfile_Id
                //result set with All Distinct Column Names along with ColumnStatisticsProfile_Id where available
                var query = from col0 in drColumnNameDistinct
                            join col1 in drColumnStatProfIds on col0.ColumnName equals col1.ColumnName into tempjoin
                            from leftJoin in tempjoin.DefaultIfEmpty()
                            select new { ColumnName = col0.ColumnName, ColumnStatisticsProfile_Id = leftJoin == null ? " "  : leftJoin.ColumnStatisticsProfile_Id };

                //result set with All Distinct Column Names and ColumnStatisticsProfile_Id along with ColumnNullRatioProfile_Id where available
                var query1 = from col0 in query
                             join col1 in drColumnNullProfIds on col0.ColumnName equals col1.ColumnName into tempjoin
                             from leftJoin in tempjoin.DefaultIfEmpty()
                             select new { col0.ColumnName, col0.ColumnStatisticsProfile_Id, ColumnNullRatioProfile_Id = leftJoin == null ? " " : leftJoin.ColumnNullRatioProfile_Id };

                //result set with All Distinct Column Names and ColumnStatisticsProfile_Id and ColumnNullRatioProfile_Id along with ColumnValueDistributionProfile_Id where available
                var query2 = from col0 in query1
                             join col1 in drColumnDistProfIds on col0.ColumnName equals col1.ColumnName into tempjoin
                             from leftJoin in tempjoin.DefaultIfEmpty()
                             select new { col0.ColumnName, col0.ColumnStatisticsProfile_Id, col0.ColumnNullRatioProfile_Id, ColumnValueDistributionProfile_Id = leftJoin == null ? " " : leftJoin.ColumnValueDistributionProfile_Id };
                //****End - Building query to fetch result set with Column Name, ColumnStatisticsProfile_Id, ColumnNullRatioProfile_Id, ColumnValueDistributionProfile_Id

                //Query to fetch NullCount from ds.Tables["ColumnNullRatioProfile"] based on ColumnNullRatioProfile_Id along with ColumnName
                var drColumnNullRatioProfile = from col0 in query2
                                            from col1 in dtColumnNullRatioProfile.AsEnumerable()
                                            where col0.ColumnNullRatioProfile_Id  == col1["ColumnNullRatioProfile_Id"].ToString() 
                                            select new
                                            {
                                                col0.ColumnName,
                                                ColumnRatioProfileId = col1.Field<Int32>("ColumnNullRatioProfile_Id").ToString(),
                                                NullCount = col1.Field<string>("NullCount")
                                            };

                //Query to fetch DistinctValue from ds.Tables["ColumnValueDistributionProfile"] based on ColumnValueDistributionProfile_Id along with ColumnName
                var drColumnValueDistributionProfile = from col0 in query2
                                                       from col2 in dtColumnValueDistributionProfile.AsEnumerable()
                                                       where col0.ColumnValueDistributionProfile_Id == col2["ColumnValueDistributionProfile_Id"].ToString()
                                                       select new
                                                       {
                                                           col0.ColumnName,
                                                           ColumnDistProfileId = col2.Field<Int32>("ColumnValueDistributionProfile_Id"),
                                                           DistinctVal = col2.Field<string>("NumberOfDistinctValues")
                                                       };

                //Query to fetch MinValue, MaxValue, Mean, StdDev from ds.Tables["ColumnStatisticsProfile"] based on ColumnStatisticsProfile_Id along with ColumnName
                var drColumnStatisticsProfile = from col0 in query2
                                                from col3 in dtColumnStatisticsProfile.AsEnumerable()
                                                where col0.ColumnStatisticsProfile_Id == col3["ColumnStatisticsProfile_Id"].ToString()
                                                select new
                                                {
                                                    col0.ColumnName,
                                                    ColumnStatProfileId = col3.Field<Int32>("ColumnStatisticsProfile_Id"),
                                                    MinValue = col3.Field<string>("MinValue"),
                                                    MaxValue = col3.Field<string>("MaxValue"),
                                                    Mean = col3.Field<string>("Mean"),
                                                    StdDev = col3.Field<string>("StdDev")
                                                };

                //****Start - Building Query for Final Result
                //query to join and fetch result set from query2 - Column Name, ColumnValueDistributionProfile_Id, ColumnStatisticsProfile_Id, ColumnNullRatioProfile_Id and drColumnNullRatioProfile - NullCount based on ColumnName
                var query3 = from col0 in query2
                             join col1 in drColumnNullRatioProfile on col0.ColumnName equals col1.ColumnName into tempjoin
                             from leftJoin in tempjoin.DefaultIfEmpty()
                             select new { col0.ColumnName, col0.ColumnValueDistributionProfile_Id, col0.ColumnStatisticsProfile_Id, col0.ColumnNullRatioProfile_Id, NullCountValue = leftJoin == null ? " " : leftJoin.NullCount };

                //query to join and fetch result set from query3 - Column Name, ColumnValueDistributionProfile_Id, ColumnStatisticsProfile_Id, ColumnNullRatioProfile_Id, NullCount and drColumnValueDistributionProfile - DistinctVal based on ColumnName
                var query4 = from col0 in query3
                             join col1 in drColumnValueDistributionProfile on col0.ColumnName equals col1.ColumnName into tempjoin
                             from leftJoin in tempjoin.DefaultIfEmpty()
                             select new { col0.ColumnName, col0.ColumnNullRatioProfile_Id, col0.ColumnValueDistributionProfile_Id, col0.ColumnStatisticsProfile_Id, col0.NullCountValue, NoofDistinctValue = leftJoin == null ? " " : leftJoin.DistinctVal };

                //query to join and fetch result set from query4 - Column Name, ColumnValueDistributionProfile_Id, ColumnStatisticsProfile_Id, ColumnNullRatioProfile_Id, NullCount, DistinctVal and drColumnStatisticsProfile - MinValue, MaximumValue, StandardDev, MeanValue based on ColumnName
                var query5 = from col0 in query4
                             join col1 in drColumnStatisticsProfile on col0.ColumnName equals col1.ColumnName into tempjoin
                             from leftJoin in tempjoin.DefaultIfEmpty()
                             select new { col0.ColumnName, col0.ColumnNullRatioProfile_Id, col0.ColumnValueDistributionProfile_Id,
                                 col0.ColumnStatisticsProfile_Id, col0.NullCountValue, col0.NoofDistinctValue,
                                 MinimumValue = leftJoin == null ? " " : leftJoin.MinValue,
                                 MaximumValue = leftJoin == null ? " " : leftJoin.MaxValue,
                                 StandardDev = leftJoin == null ? " " : leftJoin.StdDev,
                                 MeanValue = leftJoin == null ? " " : leftJoin.Mean };
                //****End - Building Query for Final Result

                // Get the Final Result set ready by populating in clsTableProfile format - ColumnName, NullCount, DistinctValue, MinValue, MaxValue, MeanValue, StdDev
                IEnumerable<clsTableProfile> queryResult = from col0 in query5
                                                           select new clsTableProfile(col0.ColumnName, col0.NullCountValue, col0.NoofDistinctValue, col0.MinimumValue, col0.MaximumValue, col0.MeanValue, col0.StandardDev);

                var jsonResult = JsonConvert.SerializeObject(queryResult, Formatting.Indented);
                response = Request.CreateResponse(HttpStatusCode.OK, jsonResult);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get database tables based on search key
        [HttpGet]
        [Route("SearchDetails/{selSearchKey?}")]
        public HttpResponseMessage GetSearchDetails(string selSearchKey="")
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                if (string.IsNullOrWhiteSpace(selSearchKey))
                {
                    sqlQuery = "SELECT SCHEMA_NAME(schema_id) AS [Schema], 'SQL Server' AS [Source],'Public',[Tables].name AS [Table],[Partitions].[rows] AS [#Rows], count(column_name) AS [#Columns], create_date AS [Create Date],modify_date AS [Modify Date], '' As [Download] FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN ( 0, 1 ) join information_schema.columns c on [Tables].name = c.TABLE_NAME and [Tables].type='U' WHERE schema_id not in (1,7) GROUP BY SCHEMA_NAME(schema_id), [Tables].name, create_date, modify_date, rows";
                }
                else
                {
                    sqlQuery = "SELECT SCHEMA_NAME(schema_id) AS [Schema], 'SQL Server' AS [Source],'Public',[Tables].name AS [Table],[Partitions].[rows] AS [#Rows], count(column_name) AS [#Columns], create_date AS [Create Date],modify_date AS [Modify Date], '' As [Download] FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN ( 0, 1 ) join information_schema.columns c on [Tables].name = c.TABLE_NAME and [Tables].type='U' WHERE schema_id not in (1,7) and [Tables].name like '%" + selSearchKey + "%' GROUP BY SCHEMA_NAME(schema_id), [Tables].name, create_date, modify_date, rows";
                }
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get object count based on search key
        [HttpGet]
        [Route("ObjectCount/{selSearchKey?}")]
        public HttpResponseMessage GetObjectCount(String selSearchKey="")
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                DataSet ds = new DataSet();

                // Create a new dataset and  DataTable for the Table count  
                DataSet dsTblCount = new DataSet();
                DataTable dtTblCount = new DataTable("TableCount");
                DataColumn dtTblCountColumn;
                DataRow dtTblCountRow;
                // Create count column  
                dtTblCountColumn = new DataColumn();
                dtTblCountColumn.DataType = typeof(string);
                dtTblCountColumn.ColumnName = "Column1";
                dtTblCountColumn.Caption = "Column1";
                dtTblCountColumn.ReadOnly = false;
                dtTblCountColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                dtTblCount.Columns.Add(dtTblCountColumn);

                SqlConnection con = new SqlConnection(connStr);  
                
                if (string.IsNullOrWhiteSpace(selSearchKey))
                {
                    sqlQuery = "SELECT SCHEMA_NAME(schema_id) AS [Schema], 'SQL Server' AS [Source],'Public',[Tables].name AS [Table],[Partitions].[rows] AS [#Rows], count(column_name) AS [#Columns], create_date AS [Create Date],modify_date AS [Modify Date], '' As [Download] FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN ( 0, 1 ) join information_schema.columns c on [Tables].name = c.TABLE_NAME and [Tables].type='U' WHERE schema_id not in (1,7) GROUP BY SCHEMA_NAME(schema_id), [Tables].name, create_date, modify_date, rows";
                }
                else
                {
                    sqlQuery = "SELECT SCHEMA_NAME(schema_id) AS [Schema], 'SQL Server' AS [Source],'Public',[Tables].name AS [Table],[Partitions].[rows] AS [#Rows], count(column_name) AS [#Columns], create_date AS [Create Date],modify_date AS [Modify Date], '' As [Download] FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN ( 0, 1 ) join information_schema.columns c on [Tables].name = c.TABLE_NAME and [Tables].type='U' WHERE schema_id not in (1,7) and [Tables].name like '%" + selSearchKey + "%' GROUP BY SCHEMA_NAME(schema_id), [Tables].name, create_date, modify_date, rows";
                }            
                con.Open();
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(dsTblCount);
                con.Close();

                dtTblCountRow = dtTblCount.NewRow();
                dtTblCountRow[0] = dsTblCount.Tables[0].Rows.Count;
                dtTblCount.Rows.Add(dtTblCountRow);
                ds.Tables.Add(dtTblCount);

                DataTable dtTransform = new DataTable();
                dtTransform.TableName = "TransformCount";
                string sqlQueryTransform = "select count(TransformId) from [AppTable].[tblTransformDetails] where Flag = 'I' ";
                if (!string.IsNullOrEmpty(selSearchKey))
                {
                    sqlQueryTransform = sqlQueryTransform + " and (TransformName like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryTransform = sqlQueryTransform + " or OutputType like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryTransform = sqlQueryTransform + " or OutputName like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryTransform = sqlQueryTransform + " or SchemaName like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryTransform = sqlQueryTransform + " or Notes like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryTransform = sqlQueryTransform + " or UserName like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryTransform = sqlQueryTransform + " or CreatedDate like '%" + selSearchKey.Trim() + "%')";
                }
                con.Open();
                SqlDataAdapter daTransform = new SqlDataAdapter(sqlQueryTransform, con);                
                daTransform.Fill(dtTransform);
                con.Close();
                ds.Tables.Add(dtTransform);

                DataTable dtIntegrate = new DataTable();
                dtIntegrate.TableName = "IntegrateCount";
                string sqlQueryIntegrate = "Select count(APIID) from [APPTable].[tblDynamicAPI] where IsDeleted = 0";
                if (!string.IsNullOrEmpty(selSearchKey))
                {
                    sqlQueryIntegrate = sqlQueryIntegrate + " and (APIName like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryIntegrate = sqlQueryIntegrate + " or APIDescription like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryIntegrate = sqlQueryIntegrate + " or UserName like '%" + selSearchKey.Trim() + "%'";
                    sqlQueryIntegrate = sqlQueryIntegrate + " or CreatedDate like '%" + selSearchKey.Trim() + "%')";
                }
                con.Open();
                SqlDataAdapter daIntegrate = new SqlDataAdapter(sqlQueryIntegrate, con);
                daIntegrate.Fill(dtIntegrate);
                con.Close();
                ds.Tables.Add(dtIntegrate);

                // Create a new DataTable for the row count    
                DataTable dtCount = new DataTable("FileCount");
                DataColumn dtCountColumn;
                DataRow dtCountRow;
                // Create count column  
                dtCountColumn = new DataColumn();
                dtCountColumn.DataType = typeof(string);
                dtCountColumn.ColumnName = "Column1";
                dtCountColumn.Caption = "Column1";
                dtCountColumn.ReadOnly = false;
                dtCountColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                dtCount.Columns.Add(dtCountColumn);

                // Create a new DataTable for storing the file details 
                DataTable Table = new DataTable("Table");
                DataColumn dtColumn;
                DataRow myDataRow;

                // Create name column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "Name";
                dtColumn.Caption = "Name";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create type column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "FileType";
                dtColumn.Caption = "FileType";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create Size column.    
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(Int32);
                dtColumn.ColumnName = "Size";
                dtColumn.Caption = "Size";
                dtColumn.AutoIncrement = false;
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                /// Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create CreatedDate column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "CreatedDate";
                dtColumn.Caption = "CreatedDate";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create LastModified column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "LastModified";
                dtColumn.Caption = "LastModified";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create Download column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "Download";
                dtColumn.Caption = "Download";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                //string StorageConnectionString="DefaultEndpointsProtocol = https; AccountName = tesserinsights; AccountKey = PEgCRIKy9Ko1rbytcZ1JOEfH29UOtMSUJlSOZulXcgpgc3IJ36LITg2Xqr396i7zK0xQsm1ZKehcVhADibwSQQ ==; EndpointSuffix = core.windows.net";
                IEnumerable<string> headerValues = Request.Headers.GetValues("username");
                string userEmail = headerValues.FirstOrDefault();
                string[] userArr = userEmail.Split('@');
                string userName;
                if (userArr[0].Contains("."))
                {
                    string[] userNameArr = userArr[0].Split('.');
                    userName = userNameArr[0]+ userNameArr[1];
                }
                else
                {
                    userName = userArr[0];
                }
                var containerName = userName.ToLower(); 

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                var container = blobClient.GetContainerReference(containerName);
                var blobLimit = 500;

                if (!container.Exists() || container == null)
                {
                    dtCountRow = dtCount.NewRow();
                    dtCountRow[0] = 0;
                    dtCount.Rows.Add(dtCountRow);
                    ds.Tables.Add(dtCount);

                    var jsonResultNoFile = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    JObject jsonNoFile = JObject.Parse(jsonResultNoFile);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonNoFile);
                    return response;
                }
                else
                {
                    var blobContinuationToken = new BlobContinuationToken();
                    BlobContinuationToken continuationToken = null;
                    do
                    {
                        var blobList = container.ListBlobsSegmented("",
                                                   true,
                                                   BlobListingDetails.Metadata,
                                                   blobLimit,
                                                   continuationToken,
                                                   new BlobRequestOptions
                                                   {
                                                       LocationMode = LocationMode.PrimaryOnly
                                                   },
                                                   null);

                        continuationToken = blobList.ContinuationToken;

                        // fetching only for BlockBlobs
                        foreach (var item in blobList.Results.OfType<CloudBlockBlob>())
                        {
                            string[] fileNmExtn = item.Name.Split('.');
                            string fileType = fileNmExtn[fileNmExtn.Length - 1];
                            string[] fileNm = fileNmExtn[0].Split('/');
                            string fileName = fileNm[fileNm.Length - 1];
                            if (!string.IsNullOrWhiteSpace(selSearchKey))
                            {
                                if (fileName.ToLower().Contains(selSearchKey.ToLower()))
                                {
                                    myDataRow = Table.NewRow();
                                    myDataRow["Name"] = fileName;
                                    myDataRow["FileType"] = fileType;
                                    myDataRow["Size"] = item.Properties.Length;
                                    myDataRow["CreatedDate"] = item.Properties.Created.ToString();
                                    myDataRow["LastModified"] = item.Properties.LastModified.ToString();
                                    Table.Rows.Add(myDataRow);
                                }
                            }
                            else
                            {
                                myDataRow = Table.NewRow();
                                myDataRow["Name"] = fileName;
                                myDataRow["FileType"] = fileType;
                                myDataRow["Size"] = item.Properties.Length;
                                myDataRow["CreatedDate"] = item.Properties.Created.ToString();
                                myDataRow["LastModified"] = item.Properties.LastModified.ToString();
                                Table.Rows.Add(myDataRow);
                            }
                        }
                    } while (continuationToken != null);
                    dtCountRow = dtCount.NewRow();
                    dtCountRow[0] = Table.Rows.Count;
                    dtCount.Rows.Add(dtCountRow);
                    ds.Tables.Add(dtCount);
                }     
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }
                
        //Download database table based on selected tablename
        [HttpGet]
        [Route("TableDownload/{selTableName}/{selSchemaName}")]
        public HttpResponseMessage GetTableDownload(string selTableName, string selSchemaName)
        {
            try
            {
                HttpResponseMessage response;
                List<string> rows = null;
                string result;
                SqlConnection con = new SqlConnection(connStr);
                con.Open();
                SqlCommand cmd = new SqlCommand("SELECT * FROM " + selSchemaName + "." + selTableName,con);
                
                using (SqlDataReader dataReader = cmd.ExecuteReader())
                {
                    rows = dataReader.ToCSV(true, ",");
                    result = string.Join("\r\n", rows);
                    dataReader.Close();
                }
                con.Close();
             
                HttpContext.Current.Response.Clear();
                HttpContext.Current.Response.Buffer = true;
                HttpContext.Current.Response.AddHeader("content-disposition", "attachment;filename=" + selTableName + ".csv");
                HttpContext.Current.Response.Charset = "";
                HttpContext.Current.Response.ContentType = "csv";// "application /text";
                HttpContext.Current.Response.Output.Write(result);
                HttpContext.Current.Response.Flush();
                HttpContext.Current.Response.End();
                HttpContext.Current.Response.Close();
                response = Request.CreateResponse(HttpStatusCode.OK, "Successfully downloaded the table " + selTableName);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get azure files from blob based on search key
        [HttpGet]
        [Route("AzureFileList/{selSearchKey?}")]
        public HttpResponseMessage GetAzureFileList(string selSearchKey="")
        {
            try
            {
                HttpResponseMessage response;
                // Create a new DataTable for storing the file details 
                DataSet ds = new DataSet();
                DataTable Table = new DataTable("Table");
                DataColumn dtColumn;
                DataRow myDataRow;

                // Create name column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "Name";
                dtColumn.Caption = "Name";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create type column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "FileType";
                dtColumn.Caption = "FileType";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create Size column.    
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(Int32);
                dtColumn.ColumnName = "Size";
                dtColumn.Caption = "Size";
                dtColumn.AutoIncrement = false;
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                /// Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create CreatedDate column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "CreatedDate";
                dtColumn.Caption = "CreatedDate";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create LastModified column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "LastModified";
                dtColumn.Caption = "LastModified";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                // Create Download column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "Download";
                dtColumn.Caption = "Download";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                //string StorageConnectionString="DefaultEndpointsProtocol = https; AccountName = tesserinsights; AccountKey = PEgCRIKy9Ko1rbytcZ1JOEfH29UOtMSUJlSOZulXcgpgc3IJ36LITg2Xqr396i7zK0xQsm1ZKehcVhADibwSQQ ==; EndpointSuffix = core.windows.net";
                IEnumerable<string> headerValues = Request.Headers.GetValues("username");
                string userEmail = headerValues.FirstOrDefault();
                string[] userArr = userEmail.Split('@');
                string userName;
                if (userArr[0].Contains("."))
                {
                    string[] userNameArr = userArr[0].Split('.');
                    userName = userNameArr[0] + userNameArr[1];
                }
                else
                {
                    userName = userArr[0];
                }
                var containerName = userName.ToLower(); 

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                var container = blobClient.GetContainerReference(containerName);
                var blobLimit = 500;
                if (!container.Exists() || container == null)
                {
                    ds.Tables.Add(Table);
                    var jsonResultContainer = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    JObject jsonContainer = JObject.Parse(jsonResultContainer);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonContainer);
                    return response;
                }
                else
                {
                    var blobContinuationToken = new BlobContinuationToken();

                    BlobContinuationToken continuationToken = null;
                    do
                    {
                        var blobList = container.ListBlobsSegmented("",
                                                   true,
                                                   BlobListingDetails.Metadata,
                                                   blobLimit,
                                                   continuationToken,
                                                   new BlobRequestOptions
                                                   {
                                                       LocationMode = LocationMode.PrimaryOnly
                                                   },
                                                   null);
                        continuationToken = blobList.ContinuationToken;

                        // fetching only for BlockBlobs
                        foreach (var item in blobList.Results.OfType<CloudBlockBlob>())
                        {
                            string[] fileNmExtn = item.Name.Split('.');
                            string fileType = fileNmExtn[fileNmExtn.Length - 1];
                            string[] fileNm = fileNmExtn[0].Split('/');
                            string fileName = fileNm[fileNm.Length - 1];
                            if (string.IsNullOrWhiteSpace(selSearchKey))
                            {
                                myDataRow = Table.NewRow();
                                myDataRow["Name"] = fileName;
                                myDataRow["FileType"] = fileType;
                                myDataRow["Size"] = item.Properties.Length;
                                myDataRow["CreatedDate"] = item.Properties.Created.ToString();
                                myDataRow["LastModified"] = item.Properties.LastModified.ToString();
                                Table.Rows.Add(myDataRow);
                            }
                            else
                            {
                                if (fileName.ToLower().Contains(selSearchKey.ToLower()))
                                {
                                    myDataRow = Table.NewRow();
                                    myDataRow["Name"] = fileName;
                                    myDataRow["FileType"] = fileType;
                                    myDataRow["Size"] = item.Properties.Length;
                                    myDataRow["CreatedDate"] = item.Properties.Created.ToString();
                                    myDataRow["LastModified"] = item.Properties.LastModified.ToString();
                                    Table.Rows.Add(myDataRow);
                                }
                            }
                        }
                    } while (continuationToken != null);
                    ds.Tables.Add(Table);
                    var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    JObject json = JObject.Parse(jsonResult);
                    response = Request.CreateResponse(HttpStatusCode.OK, json);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get all azure csv files from blob storage
        [HttpGet]
        [Route("AzureCSVFileList")]
        public HttpResponseMessage GetAzureCSVFileList()
        {
            try
            {
                HttpResponseMessage response;
                // Create a new DataTable for storing the file details 
                DataSet ds = new DataSet();
                DataTable Table = new DataTable("Table");
                DataColumn dtColumn;
                DataRow myDataRow;

                // Create name column  
                dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "Name";
                dtColumn.Caption = "Name";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                Table.Columns.Add(dtColumn);

                //string StorageConnectionString="DefaultEndpointsProtocol = https; AccountName = tesserinsights; AccountKey = PEgCRIKy9Ko1rbytcZ1JOEfH29UOtMSUJlSOZulXcgpgc3IJ36LITg2Xqr396i7zK0xQsm1ZKehcVhADibwSQQ ==; EndpointSuffix = core.windows.net";
                IEnumerable<string> headerValues = Request.Headers.GetValues("username");
                string userEmail = headerValues.FirstOrDefault();
                string[] userArr = userEmail.Split('@');
                string userName;
                if (userArr[0].Contains("."))
                {
                    string[] userNameArr = userArr[0].Split('.');
                    userName = userNameArr[0] + userNameArr[1];
                }
                else
                {
                    userName = userArr[0];
                }
                var containerName = userName.ToLower();

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                var container = blobClient.GetContainerReference(containerName);
                var blobLimit = 500;
                if (!container.Exists() || container == null)
                {
                    ds.Tables.Add(Table);
                    var jsonResultContainer = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    JObject jsonContainer = JObject.Parse(jsonResultContainer);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonContainer);
                    return response;
                }
                else
                {
                    var blobContinuationToken = new BlobContinuationToken();

                    BlobContinuationToken continuationToken = null;
                    do
                    {
                        var blobList = container.ListBlobsSegmented("",
                                                   true,
                                                   BlobListingDetails.Metadata,
                                                   blobLimit,
                                                   continuationToken,
                                                   new BlobRequestOptions
                                                   {
                                                       LocationMode = LocationMode.PrimaryOnly
                                                   },
                                                   null);
                        continuationToken = blobList.ContinuationToken;

                        // fetching only for BlockBlobs
                        foreach (var item in blobList.Results.OfType<CloudBlockBlob>())
                        {
                            string[] fileNmExtn = item.Name.Split('.');
                            string fileType = fileNmExtn[fileNmExtn.Length - 1];
                            string[] fileNm = fileNmExtn[0].Split('/');
                            string fileName = fileNm[fileNm.Length - 1];
                            if (fileType.ToLower() == "csv")
                            {
                                myDataRow = Table.NewRow();
                                myDataRow["Name"] = fileName;                                
                                Table.Rows.Add(myDataRow);
                            }                            
                        }
                    } while (continuationToken != null);
                    ds.Tables.Add(Table);
                    var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    JObject json = JObject.Parse(jsonResult);
                    response = Request.CreateResponse(HttpStatusCode.OK, json);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get Azure file preview rows based on selected filename
        [HttpGet]
        [Route("AzureFilePreview/{selFileName}")]
        public HttpResponseMessage GetAzureFilePreview(string selFileName)
        {
            try
            {
                HttpResponseMessage response;
                DataSet ds = new DataSet();
                DataTable newTable =null;

                IEnumerable<string> headerValues = Request.Headers.GetValues("username");
                string userEmail = headerValues.FirstOrDefault();
                string[] userArr = userEmail.Split('@');
                string userName;
                if (userArr[0].Contains("."))
                {
                    string[] userNameArr = userArr[0].Split('.');
                    userName = userNameArr[0] + userNameArr[1];
                }
                else
                {
                    userName = userArr[0];
                }
                var containerName = userName.ToLower();             
                string selFileDownload = selFileName + ".csv";

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(containerName);
                
                if (!cloudBlobContainer.Exists() || cloudBlobContainer == null)
                {
                    ds.Tables.Add(newTable);
                    var jsonResultContainer = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    JObject jsonContainer = JObject.Parse(jsonResultContainer);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonContainer);
                    return response;
                }
                CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(selFileDownload);
                blockBlob.FetchAttributes();
                byte[] fileBytes = new byte[blockBlob.Properties.Length];
                blockBlob.DownloadToByteArray(fileBytes, 0);

                DataTable resultData = new DataTable();
                resultData = null;
                resultData = Helper.GetDataTabletFromByteArray(fileBytes);
                if (resultData != null)
                {
                    newTable = resultData.Clone();
                    int loopcount = 20;
                    if (resultData.Rows.Count < 20)
                    {
                        loopcount = resultData.Rows.Count;
                    }
                    for (int i = 0; i < loopcount - 1; i++)
                    {
                        newTable.ImportRow(resultData.Rows[i]);
                    }             
                }
                ds.Tables.Add(newTable);
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get Azure file schema based on selected filename
        [HttpGet]
        [Route("AzureFileSchema/{selFileName}")]
        public HttpResponseMessage GetAzureFileSchema(string selFileName)
        {
            try
            {
                HttpResponseMessage response;
                string[] columnNames;
                DataSet ds = new DataSet();
                DataTable newTable = new DataTable("Table");
                DataRow myDataRow;
                // Create a column  
                DataColumn dtColumn = new DataColumn();
                dtColumn.DataType = typeof(String);
                dtColumn.ColumnName = "ColumnName";
                dtColumn.Caption = "ColumnName";
                dtColumn.ReadOnly = false;
                dtColumn.Unique = false;
                // Add column to the DataColumnCollection.  
                newTable.Columns.Add(dtColumn);

                IEnumerable<string> headerValues = Request.Headers.GetValues("username");
                string userEmail = headerValues.FirstOrDefault();
                string[] userArr = userEmail.Split('@');
                string userName;
                if (userArr[0].Contains("."))
                {
                    string[] userNameArr = userArr[0].Split('.');
                    userName = userNameArr[0] + userNameArr[1];
                }
                else
                {
                    userName = userArr[0];
                }
                var containerName = userName.ToLower();       
                string selFileDownload = selFileName + ".csv";

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(containerName);
                if (!cloudBlobContainer.Exists() || cloudBlobContainer == null)
                {
                    ds.Tables.Add(newTable);
                    var jsonResultContainer = JsonConvert.SerializeObject(ds, Formatting.Indented);
                    JObject jsonContainer = JObject.Parse(jsonResultContainer);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonContainer);
                    return response;
                }
                CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(selFileDownload);
                blockBlob.FetchAttributes();
                byte[] fileBytes = new byte[blockBlob.Properties.Length];
                blockBlob.DownloadToByteArray(fileBytes, 0);

                DataTable resultData = new DataTable();
                resultData = null;
                resultData = Helper.GetDataTabletFromByteArray(fileBytes);
                if (resultData != null)
                {
                    columnNames = (from dc in resultData.Columns.Cast<DataColumn>()
                                    select dc.ColumnName).ToArray();

                    for (int i = 0; i< columnNames.Length; i++ )
                    {
                        myDataRow = newTable.NewRow();
                        myDataRow["ColumnName"] = columnNames[i];
                        newTable.Rows.Add(myDataRow);                                
                    }                     
                }
                ds.Tables.Add(newTable);
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Download Azure file based on selected filename and file extension
        [HttpGet]
        [Route("AzureFileDownload/{selFileName}/{selFileExt}")]
        public HttpResponseMessage GetAzureFileDownload(String selFileName, String selFileExt) 
        {            
            try
            {
                IEnumerable<string> headerValues = Request.Headers.GetValues("username");
                string userEmail = headerValues.FirstOrDefault();
                string[] userArr = userEmail.Split('@');
                string userName;
                if (userArr[0].Contains("."))
                {
                    string[] userNameArr = userArr[0].Split('.');
                    userName = userNameArr[0] + userNameArr[1];
                }
                else
                {
                    userName = userArr[0];
                }
                var containerName = userName.ToLower();                
                string fileNm =  selFileName; 
                string fileNmExtn = selFileExt;  
                string selFileDownload = selFileName + "." + selFileExt;

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                CloudBlobContainer cloudBlobContainer = blobClient.GetContainerReference(containerName);
                if (!cloudBlobContainer.Exists())
                {
                    return Request.CreateResponse(HttpStatusCode.BadRequest, "Azure Container does not exist");
                }
                CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(selFileDownload);
                MemoryStream memStream = new MemoryStream();
                blockBlob.DownloadToStream(memStream);
                HttpContext.Current.Response.ContentType = fileNmExtn; 
                HttpContext.Current.Response.AddHeader("Content-Disposition", "Attachment; filename=" + blockBlob.Name.ToString());
                HttpContext.Current.Response.AddHeader("Content-Length", blockBlob.Properties.Length.ToString());
                HttpContext.Current.Response.BinaryWrite(memStream.ToArray());
                HttpContext.Current.Response.Flush();
                HttpContext.Current.Response.Close();
                HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.OK, "Successfully downloaded the file "+ selFileDownload);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        /******* Ingest Module *****/
        //Upload file to Azure and Save CSV file to Database Table if check=true. Schema name is optional
        [HttpPost] 
        [Route("UploadFileToAzure/{isChecked}/{schemaName?}")]
        public async Task<HttpResponseMessage> PostUploadFileToAzure(bool isChecked, string schemaName="") 
        {            
            try
            {
                if (!Request.Content.IsMimeMultipartContent())
                {
                    return Request.CreateResponse(HttpStatusCode.BadRequest, "File is not in proper format");
                }
                IEnumerable<string> headerValues = Request.Headers.GetValues("username");
                string userEmail = headerValues.FirstOrDefault();
                string[] userArr = userEmail.Split('@');
                string userName;
                if (userArr[0].Contains("."))
                {
                    string[] userNameArr = userArr[0].Split('.');
                    userName = userNameArr[0] + userNameArr[1];
                }
                else
                {
                    userName = userArr[0];
                }
                string fileName="";
                string containerName = userName.ToLower();

                string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                CloudBlobClient blobStorageClient = cloudStorageAccount.CreateCloudBlobClient();
                //get container reference ,create new if it does not exists
                var container = blobStorageClient.GetContainerReference(containerName);

                container.CreateIfNotExists();

                var filesReadToProvider = await Request.Content.ReadAsMultipartAsync();
                
                foreach (var stream in filesReadToProvider.Contents)
                {
                    string fileNameStr = stream.Headers.ContentDisposition.FileName;
                    fileNameStr = fileNameStr.Replace('\"', ' ');
                    fileName = fileNameStr.Trim();
                    var blobRef = container.GetBlockBlobReference(fileName);              

                    var fileBytes = await stream.ReadAsByteArrayAsync();
                    blobRef.UploadFromByteArray(fileBytes,0, fileBytes.Length-1);

                    if (isChecked)
                    {
                        var fname = Path.GetFileNameWithoutExtension(fileName);
                        var fext = Path.GetExtension(fileName);

                        if (fext != null)
                        {
                            if (fext.ToString().ToUpper() == ".CSV" || fext.ToString().ToUpper() == ".TXT")
                            {
                                DataTable resultData = new DataTable();
                                resultData = null;
                                resultData = Helper.GetDataTabletFromByteArray(fileBytes);
                                if (resultData != null)
                                {
                                    Helper.InsertDataIntoSQLServerUsingSQLBulkCopy(resultData, fname, schemaName);
                                }
                            }
                        }
                    }
                }
                HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.OK, "Successfully uploaded the file");
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }


        // **************Integration Module ***************//
        // Get the list of all APIs Data so for created  
        [Route("APIDataList/{SearchText?}")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetAPIDataList(string SearchText="")
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery = " ";
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "Select APIID,APIName,APIDescription,SchemaName,TableName,InputColumns,OutputColumns,UserName,CreatedDate from [APPTable].[tblDynamicAPI] where IsDeleted = 0";
                if (!string.IsNullOrEmpty(SearchText))
                {
                    sqlQuery = sqlQuery + " and (APIName like '%" + SearchText.Trim() + "%'";
                    sqlQuery = sqlQuery + " or APIDescription like '%" + SearchText.Trim() + "%'";
                    sqlQuery = sqlQuery + " or UserName like '%" + SearchText.Trim() + "%'";
                    sqlQuery = sqlQuery + " or CreatedDate like '%" + SearchText.Trim() + "%')";
                }
                sqlQuery = sqlQuery + " Order by APIID DESC";
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                List<APIData> Apidata = new List<APIData>();

                if (ds != null)
                {
                    if (ds.Tables.Count > 0)
                    {
                        if (ds.Tables[0].Rows.Count > 0)
                        {
                            foreach (DataRow dr in ds.Tables[0].Rows)
                            {
                                string strURL = Request.RequestUri.ToString().Substring(0, Request.RequestUri.ToString().IndexOf("APIDataList"));
                                APIData data = new APIData()
                                {
                                    APIID = (int)dr["APIID"],
                                    APIName = dr["APIName"].ToString(),
                                    APIDescription = dr["APIDescription"].ToString(),
                                    SchemaName = dr["SchemaName"].ToString(),
                                    TableName = dr["TableName"].ToString(),
                                    InputColumns = dr["InputColumns"].ToString(),
                                    OutputColumns = dr["OutputColumns"].ToString(),
                                    UserName = dr["UserName"].ToString(),
                                    CreatedDate = dr["CreatedDate"].ToString(),
                                    APIURL = strURL + "DatafromAzure/" + dr["APIID"].ToString()
                                };
                                Apidata.Add(data);
                            }
                        }
                        //else
                        //    return Request.CreateErrorResponse(HttpStatusCode.NoContent, "No Data Found");
                    }
                    else
                        return Request.CreateErrorResponse(HttpStatusCode.NoContent, "No Data Found");

                }
                var jsonResult = JsonConvert.SerializeObject(Apidata, Formatting.Indented);
                //JObject json = JObject.Parse(jsonResult);
                return Request.CreateErrorResponse(HttpStatusCode.OK, jsonResult);
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        // **************Integration Module ***************//
        // Delete APIs Data based on API ID  
        [Route("APIDelete/{TemplateID}")]
        [HttpGet]
        public async Task<HttpResponseMessage> GetAPIDelete(string TemplateID)
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery = " ";
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "Update [AppTable].[tblDynamicAPI] set IsDeleted = 1 where ApiID = @APIID ";
                using (SqlCommand cmd = new SqlCommand(sqlQuery, con))
                {
                    cmd.Parameters.AddWithValue("@APIID", TemplateID);
                    cmd.ExecuteScalar();
                    if (con.State == System.Data.ConnectionState.Open)
                        con.Close();

                }
                return response = Request.CreateResponse(HttpStatusCode.OK, "Data Deleted");
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        // **************Integration Module ***************//
        // Get APIsfrom Azure 
        [Route("APIfromAzure")]
        [HttpPost]
        public async Task<HttpResponseMessage> PostAPIfromAzure()
        {
            try
            {
                HttpResponseMessage response;
                string strReqBody = await Request.Content.ReadAsStringAsync();

                string sqlQuery = "";
                string APIName = "";
                string APIDescription = "";
                String Schema = "";
                String TableName = "";
                String InputCols = "";
                String OutputCols = "";
                string UserName = "";
                var ReqBody = JObject.Parse(strReqBody);
                if (ReqBody.ContainsKey("APIName"))
                    APIName = ReqBody["APIName"].ToString();
                if (ReqBody.ContainsKey("APIDescription"))
                    APIDescription = ReqBody["APIDescription"].ToString();
                if (ReqBody.ContainsKey("SchemaName"))
                    Schema = ReqBody["SchemaName"].ToString();
                if (ReqBody.ContainsKey("TableName"))
                    TableName = ReqBody["TableName"].ToString();
                if (ReqBody.ContainsKey("InputColumns"))
                    InputCols = ReqBody["InputColumns"].ToString();
                if (ReqBody.ContainsKey("OutputColumns"))
                    OutputCols = ReqBody["OutputColumns"].ToString();
                if (ReqBody.ContainsKey("UserName"))
                    UserName = ReqBody["UserName"].ToString();

                if (string.IsNullOrEmpty(APIName) || string.IsNullOrEmpty(Schema) || string.IsNullOrEmpty(TableName) || string.IsNullOrEmpty(InputCols) || string.IsNullOrEmpty(OutputCols))
                    return Request.CreateResponse(HttpStatusCode.BadRequest, "Input value missing");

                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "Select * from [APPTable].[tblDynamicAPI] where IsDeleted = 0 and APIName = '" + APIName + "'";

                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                if (ds != null)
                {
                    if (ds.Tables.Count > 0)
                    {
                        if (ds.Tables[0].Rows.Count > 0)
                            return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Data already available with API Name " + APIName + ". Can you please try with different API name");
                    }
                }
                //con.Close();

                DataSet dsTemplate = new DataSet();

                sqlQuery = "INSERT INTO [AppTable].[tblDynamicAPI] ( APIName,APIDescription,SchemaName, TableName, InputColumns,OutputColumns,UserName,CreatedDate,IsDeleted) Values(@APIName,@APIDescription,@Schema, @TableName, @InputCols, @OutputCols,@UserName,GetDate(),0); SELECT SCOPE_IDENTITY(); ";
                int ID = 0;
                using (SqlCommand cmd = new SqlCommand(sqlQuery, con))
                {
                    cmd.Parameters.AddWithValue("@APIName", APIName);
                    cmd.Parameters.AddWithValue("@APIDescription", APIDescription);
                    cmd.Parameters.AddWithValue("@Schema", Schema);
                    cmd.Parameters.AddWithValue("@TableName", TableName);
                    cmd.Parameters.AddWithValue("@InputCols", InputCols);
                    cmd.Parameters.AddWithValue("@OutputCols", OutputCols);
                    cmd.Parameters.AddWithValue("@UserName", UserName);
                    //con.Open();

                    ID = Convert.ToInt32(cmd.ExecuteScalar());

                    if (con.State == System.Data.ConnectionState.Open)
                        con.Close();

                }
                string url = "";
                ProductApi result = new ProductApi();
                if (ID != 0)
                {
                    result.APIURL = Request.RequestUri.ToString().Replace("APIfromAzure", "DatafromAzure/" + ID);
                }
                string APIPostString = "";
                string[] InputColumns = InputCols.Split(',');
                if (InputColumns.Length > 0)
                {
                    APIPostString = "{";
                    int icount = 0;
                    foreach (string iColumn in InputColumns)
                    {
                        if (icount == InputColumns.Length - 1)
                        { APIPostString = APIPostString + "\"" + iColumn + "\":\"\" "; }
                        else
                        { APIPostString = APIPostString + "\"" + iColumn + "\":\"\" ,"; }
                        //APIPostString = APIPostString + "\"" + iColumn + "\":\"\" ,";
                        icount++;
                    }
                    APIPostString = APIPostString + "}";
                }

                result.APIPostText = APIPostString;
                var jsonResult = JsonConvert.SerializeObject(result, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);

                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }
        // **************Integration Module ***************//
        // Get the all data from database based on api
        [Route("DatafromAzure/{TemplateID}")]
        [HttpPost]
        public async Task<HttpResponseMessage> PostDatafromAzure(string TemplateID)
        {
            try
            {
                string strReqBody = await Request.Content.ReadAsStringAsync();
                string sqlQuery;
                HttpResponseMessage response;
                SqlConnection con = new SqlConnection(connStr);
                DataSet dsTemplate = new DataSet();
                con.Open();
                sqlQuery = "Select SchemaName, TableName,InputColumns, OutputColumns from [AppTable].[tblDynamicAPI] where APIID =" + TemplateID;
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(dsTemplate);

                if (dsTemplate != null)
                {
                    if (dsTemplate.Tables.Count > 0)
                    {
                        if (dsTemplate.Tables[0].Rows.Count > 0)
                        {
                            string Query = "Select ";
                            var TemplateRow = dsTemplate.Tables[0].Rows[0];
                            String Schema = TemplateRow["SchemaName"].ToString();
                            String TableName = TemplateRow["TableName"].ToString();
                            String InputCols = TemplateRow["InputColumns"].ToString();
                            String OutputCols = TemplateRow["OutputColumns"].ToString();

                            string[] InputColumns = InputCols.Split(',');
                            string[] OutputColumns = OutputCols.Split(',');

                            if (InputColumns.Length <= 0)
                            {
                                return Request.CreateResponse("Input Columns Required");
                            }

                            int icount = 0;
                            foreach (string oColumn in OutputColumns)
                            {
                                if (icount == OutputColumns.Length - 1)
                                { Query = Query + "[" + oColumn.Trim() + "] "; }
                                else
                                { Query = Query + "[" + oColumn.Trim() + "] ,"; }
                                icount++;
                            }
                            //var lastComma = Query.LastIndexOf(',');
                            //if (lastComma != -1) Query = Query.Remove(lastComma, 1).Insert(lastComma, " ");

                            Query = Query + "From [" + Schema + "].[" + TableName + "]";

                            if (InputColumns.Length > 0)
                            {
                                Query = Query + " Where 1=1 ";
                                foreach (string iColumn in InputColumns)
                                {
                                    var ReqBody = JObject.Parse(strReqBody);
                                    if (ReqBody.ContainsKey(iColumn))
                                    {
                                        var RValue = ReqBody[iColumn].ToString();
                                        if (RValue != null)
                                        {
                                            Query = Query + "and [" + iColumn + "]= '" + RValue + "'";
                                        }
                                    }
                                    else
                                        return Request.CreateResponse(HttpStatusCode.BadRequest, "Input Column missing ");
                                }
                                //lastComma = Query.LastIndexOf(',');
                                //if (lastComma != -1) Query = Query.Remove(lastComma, 1).Insert(lastComma, " ");
                            }
                            DataSet ds = new DataSet(TableName);
                            SqlDataAdapter daResult = new SqlDataAdapter(Query, con);
                            daResult.Fill(ds, "Table");

                            var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                            JObject json = JObject.Parse(jsonResult);
                            return Request.CreateResponse(HttpStatusCode.OK, json);
                        }
                        else
                            return Request.CreateResponse(HttpStatusCode.BadRequest, "No Data found");
                    }
                    else
                        return Request.CreateResponse(HttpStatusCode.BadRequest, "No Data found");
                }
                else
                    return Request.CreateResponse(HttpStatusCode.BadRequest, "No Data found");

                con.Close();
                response = Request.CreateResponse(HttpStatusCode.OK);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //******* Transform Module *****//
        //Get all the available schema from database
        [HttpGet]
        [Route("SchemaListTransform")]
        public HttpResponseMessage GetSchemaListTransform()
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "Select distinct schema_name(schema_id), schema_id FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN (0, 1) join information_schema.columns c on [Tables].name = c.TABLE_NAME and [Tables].type = 'U' where schema_id not in (1,7)";
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get all the tables from database for the selected schema id
        [HttpGet]
        [Route("TableListTransform/{schemaId}")]
        public HttpResponseMessage GetTableListTransform(int schemaId)
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "SELECT distinct [Tables].name AS [Table] FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN (0, 1) join information_schema.columns c on [Tables].name = c.TABLE_NAME and [Tables].type = 'U' WHERE schema_id = "+ schemaId;
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Get all the columns for selected table and schema id from database
        [HttpGet]
        [Route("ColumnListTransform/{schemaId}/{tableName}")]
        public HttpResponseMessage GetColumnListTransform(int schemaId, string tableName)
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where table_name in (select name from sys.tables where type = 'U' and schema_id = " + schemaId+" and name = '"+tableName+ "') and DATA_TYPE != 'uniqueidentifier'";
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        [HttpPut]
        [Route("ColumnMaskTransform/{schemaName}/{tableName}/{ColName}")]
        public HttpResponseMessage PutColumnMaskTransform(string schemaName, string tableName, string ColName)
        {
            try
            {
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = string.Format("ALTER TABLE {0} ALTER COLUMN {1} ADD MASKED WITH (FUNCTION = 'default()')", schemaName+"."+ tableName, ColName);
                SqlCommand cmd = new SqlCommand(sqlQuery, con);
                cmd.ExecuteNonQuery();
                con.Close();
                HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.OK, "Successfully Masked the column "+ ColName);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //View Transform - Get request object from [AppTable].[tblTransformDetails] based on TransformId
        [HttpGet]
        [Route("TransformIdView/{transformId}")]
        public HttpResponseMessage GetTransformIdView(int transformId)
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "SELECT RequestObject FROM [AppTable].[tblTransformDetails] where Flag = 'I' and TransformId = " + transformId;
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Delete Transform - Soft Delete from [AppTable].[tblTransformDetails] based on TransformId
        [HttpPut]
        [Route("TransformIdDelete/{transformId}")]
        public HttpResponseMessage PutTransformIdDelete(int transformId)
        {
            try
            {
                int rowsAffected;
                HttpResponseMessage response;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();                
                using (SqlCommand cmd = new SqlCommand("UPDATE [AppTable].[tblTransformDetails] SET Flag = @flag WHERE TransformId = @TransformId", con))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@flag", 'D');
                    cmd.Parameters.AddWithValue("@TransformId", transformId);
                    con.Open();
                    rowsAffected = cmd.ExecuteNonQuery();
                    con.Close();
                }
                if (rowsAffected > 0)
                {
                    response = Request.CreateResponse(HttpStatusCode.OK, "Successfully deleted the transform "+transformId);
                }
                else
                {
                    response = Request.CreateResponse(HttpStatusCode.NotImplemented, "Sorry, unable to delete transform " + transformId);
                }                
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }

        }

        //Search and List Transform - Get list of transformations based on search key
        [HttpGet]
        [Route("TransformList/{selSearchKey?}")]
        public HttpResponseMessage GetTransformList(string selSearchKey="")
        {
            try
            {
                HttpResponseMessage response;
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                con.Open();
                sqlQuery = "select TransformId,TransformName,OutputType,OutputName,SchemaName,CreatedDate,UserName,Notes from [AppTable].[tblTransformDetails] where Flag='I'";
                if (!string.IsNullOrEmpty(selSearchKey))
                {
                    sqlQuery = sqlQuery + " and (TransformName like '%" + selSearchKey.Trim() + "%'";
                    sqlQuery = sqlQuery + " or OutputType like '%" + selSearchKey.Trim() + "%'";
                    sqlQuery = sqlQuery + " or OutputName like '%" + selSearchKey.Trim() + "%'";
                    sqlQuery = sqlQuery + " or SchemaName like '%" + selSearchKey.Trim() + "%'";
                    sqlQuery = sqlQuery + " or Notes like '%" + selSearchKey.Trim() + "%'";
                    sqlQuery = sqlQuery + " or UserName like '%" + selSearchKey.Trim() + "%'";
                    sqlQuery = sqlQuery + " or CreatedDate like '%" + selSearchKey.Trim() + "%')";
                }
                sqlQuery = sqlQuery + "order by TransformId desc";
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                var jsonResult = JsonConvert.SerializeObject(ds, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Run existing Transform - Run the query of existing transformation based on transform id
        [HttpGet]
        [Route("TransformRun/{transformId}")]
        public HttpResponseMessage GetTransformRun(int transformId)
        {
            try
            {
                HttpResponseMessage response;
                SqlConnection con = new SqlConnection(connStr);
                // get transform details (including transformation query) from [AppTable].[tblTransformDetails] for transform Id 
                string sqlQuery;
                DataSet dsTrnsfQuery = new DataSet();                
                sqlQuery = "select [TransformQuery],[OutputType],[OutputName],[SchemaName],[UserName] from [AppTable].[tblTransformDetails] where TransformId = " + transformId;
                con.Open();
                SqlDataAdapter daTrnsfQuery = new SqlDataAdapter(sqlQuery, con);
                daTrnsfQuery.Fill(dsTrnsfQuery);
                con.Close();

                //execute the transformation query thus obtained and store the result in dataset
                string sqlQueryRunTransform = dsTrnsfQuery.Tables[0].Rows[0]["TransformQuery"].ToString();
                DataSet dsTransformResult = new DataSet();                
                con.Open();
                SqlDataAdapter daTransformResult = new SqlDataAdapter(sqlQueryRunTransform, con);
                daTransformResult.Fill(dsTransformResult);
                con.Close();

                // overwrite as sql table or azure file
                string outType = dsTrnsfQuery.Tables[0].Rows[0]["OutputType"].ToString().ToLower();
                DataTable dtOutput = new DataTable();
                dtOutput = dsTransformResult.Tables[0];
                if (outType == "table")
                {
                    if (dtOutput != null)
                    {
                        string sqlTableName = dsTrnsfQuery.Tables[0].Rows[0]["OutputName"].ToString();
                        string sqlSchemaName = Convert.ToString(ConfigurationManager.AppSettings["SqlSchemaSandbox"]);

                        con.Open();
                        string cmdText = "DROP TABLE IF EXISTS["+ sqlSchemaName+"].["+ sqlTableName+"]";
                        SqlCommand cmd = new SqlCommand(cmdText, con);
                        cmd.ExecuteNonQuery();
                        con.Close();

                        // store the result datatable in SQL server database 
                        Helper.InsertDataIntoSQLServerUsingSQLBulkCopy(dtOutput, sqlTableName, sqlSchemaName);
                    }
                }
                else if (outType == "file")
                {
                    string azureFileName = dsTrnsfQuery.Tables[0].Rows[0]["OutputName"].ToString() + ".csv";
                    string userEmail = dsTrnsfQuery.Tables[0].Rows[0]["UserName"].ToString();

                    string[] userArr = userEmail.Split('@');
                    string userName;
                    if (userArr[0].Contains("."))
                    {
                        string[] userNameArr = userArr[0].Split('.');
                        userName = userNameArr[0] + userNameArr[1];
                    }
                    else
                    {
                        userName = userArr[0];
                    }
                    string containerName = userName.ToLower();

                    string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                    CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                    CloudBlobClient blobStorageClient = cloudStorageAccount.CreateCloudBlobClient();
                    //get container reference ,create new if it does not exists
                    var container = blobStorageClient.GetContainerReference(containerName);

                    container.CreateIfNotExists();

                    var blockBlob = container.GetBlockBlobReference(azureFileName);
                    blockBlob.DeleteIfExists();
                    byte[] blobBytes;

                    // read the datatable into memory stream using streamwriter
                    using (var writeStream = new MemoryStream())
                    {
                        using (var swriter = new StreamWriter(writeStream))
                        {
                            //headers  
                            for (int i = 0; i < dtOutput.Columns.Count; i++)
                            {
                                swriter.Write(dtOutput.Columns[i]);
                                if (i < dtOutput.Columns.Count - 1)
                                {
                                    swriter.Write(",");
                                }
                            }
                            swriter.Write(swriter.NewLine);
                            foreach (DataRow dr in dtOutput.Rows)
                            {
                                for (int i = 0; i < dtOutput.Columns.Count; i++)
                                {
                                    if (!Convert.IsDBNull(dr[i]))
                                    {
                                        string value = dr[i].ToString();
                                        if (value.Contains(','))
                                        {
                                            value = String.Format("\"{0}\"", value);
                                            swriter.Write(value);
                                        }
                                        else
                                        {
                                            swriter.Write(dr[i].ToString());
                                        }
                                    }
                                    if (i < dtOutput.Columns.Count - 1)
                                    {
                                        swriter.Write(",");
                                    }
                                }
                                swriter.Write(swriter.NewLine);
                            }
                            swriter.Close();
                        }
                        blobBytes = writeStream.ToArray();
                    }
                    using (var readStream = new MemoryStream(blobBytes))
                    {
                        blockBlob.UploadFromStream(readStream);
                    }
                }
                response = Request.CreateResponse(HttpStatusCode.OK, "Successfully executed the transform "+transformId);
                return response;
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Query building on the selected tables and column filter using inner join. Execute and save the result in SQL table or Azure blob. Enter the Meta data details in SQL table [AppTable].[tblTransformDetails]
        [HttpPost]
        [Route("SaveQueryTransform")]
        public async Task<HttpResponseMessage> PostSaveQueryTransform()
        {
            try
            {
                HttpResponseMessage response;
                // Create a new DataTable for storing the response TransformId  
                DataSet dsTransformId = new DataSet();
                DataTable dtTransformId = new DataTable();
                DataColumn dcTransformId;
                DataRow drTransformId;
                // Create TransformId column  
                dcTransformId = new DataColumn();
                dcTransformId.DataType = typeof(Int32);
                dcTransformId.ColumnName = "Column1";
                dcTransformId.Caption = "Column1";
                dcTransformId.ReadOnly = false;
                dcTransformId.Unique = false;
                // Add column to the DataColumnCollection.  
                dtTransformId.Columns.Add(dcTransformId);

                // read the request object in Rootobject class structure
                string strReqBody = await Request.Content.ReadAsStringAsync();
                Rootobject item = JsonConvert.DeserializeObject<Rootobject>(strReqBody);
                string sqlSchemaName = Convert.ToString(ConfigurationManager.AppSettings["SqlSchemaSandbox"]); 
                // build query and get result in dataset
                string sqlQuery;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                int tableArrCount = item.tables.Count;
                int joinArrCount = item.joins.Count;
                
                sqlQuery = " SELECT ";

                //loop for getting the select columns fields
                for (int tableLoop = 0; tableLoop < tableArrCount; tableLoop++)
                {
                    for (int columnLoop = 0; columnLoop < item.tables[tableLoop].columns.Count; columnLoop++)
                    {
                        if (columnLoop > 0 || tableLoop > 0)
                        {
                            sqlQuery = sqlQuery + " , ";
                        }
                        sqlQuery = sqlQuery + "["+ item.tables[tableLoop].tableNm + "].[" + item.tables[tableLoop].columns[columnLoop] + "]";
                    }
                }

                sqlQuery = sqlQuery + " FROM " + "[" + item.tables[0].schemaNm + "].[" + item.tables[0].tableNm + "] inner JOIN [" + item.tables[1].schemaNm + "].[" + item.tables[1].tableNm + "] ON ";

                //loop for getting the join conditions
                for (int i = 0; i < joinArrCount; i++)
                {
                    for (int j = 0; j < item.joins[i].columns.Count; j++)
                    {
                        if (j > 0 || i > 0)
                        {
                            sqlQuery = sqlQuery + " and ";
                        }
                        sqlQuery = sqlQuery + "[" + item.joins[i].table1.name + "].[" + item.joins[i].columns[j].column1 + "] = [" + item.joins[i].table2.name + "].[" + item.joins[i].columns[j].column2 + "]";
                    }
                }                
                
                //loop for getting the filter conditions
                for (int j = 0; j < tableArrCount; j++)
                {                      
                    int filterArrCount = item.tables[j].filters.Count;
                    for (int k = 0; k < filterArrCount; k++)
                    {
                        int alphaCount = Regex.Matches(item.tables[j].filters[k].value, @"[a-zA-Z]").Count;

                        if ((!string.IsNullOrEmpty(item.tables[j].filters[k].columnNm.Trim())) && (!string.IsNullOrEmpty(item.tables[j].filters[k].operatorKey.Trim())) && (!string.IsNullOrEmpty(item.tables[j].filters[k].value.Trim())))
                        {

                            if (j == 0 && k == 0)
                            {
                                sqlQuery = sqlQuery + " where ";
                            }
                            else
                            {
                                sqlQuery = sqlQuery + " and ";
                            }
                        

                            if ((alphaCount > 0) && (item.tables[j].filters[k].operatorKey.ToString() == "="))
                            {
                                sqlQuery = sqlQuery + " [" + item.tables[j].schemaNm + "].[" + item.tables[j].tableNm + "].[" + item.tables[j].filters[k].columnNm + "] " + item.tables[j].filters[k].operatorKey + " '" + item.tables[j].filters[k].value + "' ";
                            }
                            else if (item.tables[j].filters[k].operatorKey.ToString() == "like")
                            {
                                sqlQuery = sqlQuery + " [" + item.tables[j].schemaNm + "].[" + item.tables[j].tableNm + "].[" + item.tables[j].filters[k].columnNm + "] " + item.tables[j].filters[k].operatorKey + " '%" + item.tables[j].filters[k].value + "%' ";
                            }
                            else
                            {
                                sqlQuery = sqlQuery + " [" + item.tables[j].schemaNm + "].[" + item.tables[j].tableNm + "].[" + item.tables[j].filters[k].columnNm + "] " + item.tables[j].filters[k].operatorKey + " " + item.tables[j].filters[k].value;
                            }

                        }

                    }

                }
                con.Open();
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                DataTable dtOutput = ds.Tables[0];

                //eliminate the rowguid column from datatable
                for (var i = dtOutput.Columns.Count - 1; i >= 0; i--)
                {
                    if (dtOutput.Columns[i].ColumnName.ToLower().Contains("guid"))
                    {
                        dtOutput.Columns.Remove(dtOutput.Columns[i].ColumnName);
                    }
                }

                //check if the transformation name already exist, if yes return error message          
                string sqlTransformExist = "select * from [AppTable].[tblTransformDetails] where TransformName = '" + item.transformationName.ToString() + "'";
                DataSet dsTransformExist = new DataSet();
                con.Open();
                SqlDataAdapter daTransformExist = new SqlDataAdapter(sqlTransformExist, con);
                daTransformExist.Fill(dsTransformExist);
                con.Close();
                if (dsTransformExist.Tables[0].Rows.Count > 0)
                {
                    return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Transformation already exist, please enter a New Transformation name");
                }

                // save as sql table or azure file
                string outType = item.output.outputType.ToString().ToLower();
                if (outType == "table")
                {
                    if (dtOutput != null)
                    {
                        string sqlTableName = item.output.tableName.ToString();      
                        string cmdText = @"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + sqlSchemaName + "' and TABLE_NAME='" + sqlTableName + "') SELECT 1 ELSE SELECT 0";
                        con.Open();
                        SqlCommand cmd = new SqlCommand(cmdText, con);
                        int x = Convert.ToInt32(cmd.ExecuteScalar());
                        if (x == 1)
                            return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Table already exist, please enter a New table name");
                        con.Close();


                        // store the result datatable in SQL server database 
                        Helper.InsertDataIntoSQLServerUsingSQLBulkCopy(dtOutput, sqlTableName, sqlSchemaName);
                    }
                }
                else if (outType == "file")
                {
                    string azureFileName = item.output.fileName.ToString()+".csv";
                    string userEmail = item.userName.ToString();

                    string[] userArr = userEmail.Split('@');
                    string userName;
                    if (userArr[0].Contains("."))
                    {
                        string[] userNameArr = userArr[0].Split('.');
                        userName = userNameArr[0] + userNameArr[1];
                    }
                    else
                    {
                        userName = userArr[0];
                    }
                    string containerName = userName.ToLower();

                    string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                    CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                    CloudBlobClient blobStorageClient = cloudStorageAccount.CreateCloudBlobClient();
                    //get container reference ,create new if it does not exists
                    var container = blobStorageClient.GetContainerReference(containerName);

                    container.CreateIfNotExists();

                    var blockBlob = container.GetBlockBlobReference(azureFileName);
                    if (blockBlob.Exists())
                    {
                        return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "File already exist, please enter a New file name");
                    }

                    byte[] blobBytes;

                    // read the datatable into memory stream using streamwriter
                    using (var writeStream = new MemoryStream())
                    {
                        using (var swriter = new StreamWriter(writeStream))
                        {
                            //headers  
                            for (int i = 0; i < dtOutput.Columns.Count; i++)
                            {
                                swriter.Write(dtOutput.Columns[i]);
                                if (i < dtOutput.Columns.Count - 1)
                                {
                                    swriter.Write(",");
                                }
                            }
                            swriter.Write(swriter.NewLine);
                            foreach (DataRow dr in dtOutput.Rows)
                            {
                                for (int i = 0; i < dtOutput.Columns.Count; i++)
                                {
                                    if (!Convert.IsDBNull(dr[i]))
                                    {
                                        string value = dr[i].ToString();
                                        if (value.Contains(','))
                                        {
                                            value = String.Format("\"{0}\"", value);
                                            swriter.Write(value);
                                        }
                                        else
                                        {
                                            swriter.Write(dr[i].ToString());
                                        }
                                    }
                                    if (i < dtOutput.Columns.Count - 1)
                                    {
                                        swriter.Write(",");
                                    }
                                }
                                swriter.Write(swriter.NewLine);
                            }
                            swriter.Close();
                        }                    
                        blobBytes = writeStream.ToArray();
                    }
                    using (var readStream = new MemoryStream(blobBytes))
                    {
                        blockBlob.UploadFromStream(readStream);
                    }             
                }

                // insert the meta data information of this transformation in SQL server [AppTable].[tblTransformDetails] table
                string sqlQueryInsert = "INSERT INTO [AppTable].[tblTransformDetails](TransformName,RequestObject,TransformQuery,OutputType,OutputName,SchemaName,UserName,Notes,Flag) VALUES(@TransformName,@RequestObject,@TransformQuery,@OutputType,@OutputName,@SchemaName,@UserName,@Notes,@Flag); SELECT SCOPE_IDENTITY(); ";
                int transformId = 0;
                using (SqlCommand cmd = new SqlCommand(sqlQueryInsert, con))
                {
                    cmd.Parameters.AddWithValue("@TransformName", item.transformationName.ToString());
                    cmd.Parameters.AddWithValue("@RequestObject", strReqBody);
                    cmd.Parameters.AddWithValue("@TransformQuery", sqlQuery);
                    cmd.Parameters.AddWithValue("@OutputType", item.output.outputType);
                    if (outType == "table")
                    {
                        cmd.Parameters.AddWithValue("@OutputName", item.output.tableName.ToString());
                        cmd.Parameters.AddWithValue("@SchemaName", sqlSchemaName);
                    }
                    else if (outType == "file")
                    {
                        cmd.Parameters.AddWithValue("@OutputName", item.output.fileName.ToString());
                        cmd.Parameters.AddWithValue("@SchemaName", string.Empty);
                    }
                    cmd.Parameters.AddWithValue("@UserName", item.userName);
                    cmd.Parameters.AddWithValue("@Notes", item.notes);
                    cmd.Parameters.AddWithValue("@Flag", "I");
                    con.Open();

                    // after insert, store the TransformId auto generated in the variable transformId
                    transformId = Convert.ToInt32(cmd.ExecuteScalar());

                    if (con.State == System.Data.ConnectionState.Open)
                        con.Close();
                }
                drTransformId = dtTransformId.NewRow();
                drTransformId[0] = transformId;
                dtTransformId.Rows.Add(drTransformId);
                dsTransformId.Tables.Add(dtTransformId);

                var jsonResult = JsonConvert.SerializeObject(dsTransformId, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (SqlException exception)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Please give valid values for transformation.");
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Query building on the selected tables and column filter using inner join. Execute and save the result in SQL table or Azure blob. Enter the Meta data details in SQL table [AppTable].[tblTransformDetails]
        [HttpPost]
        [Route("SaveTransform")]
        public async Task<HttpResponseMessage> PostSaveTransform()
        {
            try
            {
                HttpResponseMessage response;
                // Create a new DataTable for storing the response TransformId  
                DataSet dsTransformId = new DataSet();
                DataTable dtTransformId = new DataTable();
                DataColumn dcTransformId;
                DataRow drTransformId;
                // Create TransformId column  
                dcTransformId = new DataColumn();
                dcTransformId.DataType = typeof(Int32);
                dcTransformId.ColumnName = "Column1";
                dcTransformId.Caption = "Column1";
                dcTransformId.ReadOnly = false;
                dcTransformId.Unique = false;
                // Add column to the DataColumnCollection.  
                dtTransformId.Columns.Add(dcTransformId);

                // read the request object in Rootobject class structure
                string strReqBody = await Request.Content.ReadAsStringAsync();
                Rootobject item = JsonConvert.DeserializeObject<Rootobject>(strReqBody);
                string sqlSchemaName = Convert.ToString(ConfigurationManager.AppSettings["SqlSchemaSandbox"]);
                // build query and get result in dataset
                string sqlQuery;
                string sqlQueryJoin;
                string sqlQueryNonAggregCol = string.Empty;
                string sqlQueryAggregateCol = string.Empty;
                string sqlQueryFilter = string.Empty;
                SqlConnection con = new SqlConnection(connStr);
                DataSet ds = new DataSet();
                int tableArrCount = item.tables.Count;
                int joinArrCount = item.joins.Count;

                //forming the query string with the - non-aggregate columns fields in select, filter columns, aggregate column fields in select
                //loop the tables for getting the - non-aggregate columns fields in select, filter columns, aggregate column fields in select
                for (int tableLoop = 0; tableLoop < tableArrCount; tableLoop++)
                {

                    //form the query string with non-aggregate columns fields, for select and groupby clauses
                    for (int columnLoop = 0; columnLoop < item.tables[tableLoop].columns.Count; columnLoop++)
                    {
                        if (!string.IsNullOrEmpty(sqlQueryNonAggregCol))
                        {
                            sqlQueryNonAggregCol = sqlQueryNonAggregCol + " , ";
                        }
                        sqlQueryNonAggregCol = sqlQueryNonAggregCol + "[" + item.tables[tableLoop].tableNm + "].[" + item.tables[tableLoop].columns[columnLoop] + "]";
                    }

                    //form the query string with filter columns 
                    int filterArrCount = item.tables[tableLoop].filters.Count;
                    for (int k = 0; k < filterArrCount; k++)
                    {
                        int alphaCount = Regex.Matches(item.tables[tableLoop].filters[k].value, @"[a-zA-Z]").Count;

                        if ((!string.IsNullOrEmpty(item.tables[tableLoop].filters[k].columnNm.Trim())) && (!string.IsNullOrEmpty(item.tables[tableLoop].filters[k].operatorKey.Trim())) && (!string.IsNullOrEmpty(item.tables[tableLoop].filters[k].value.Trim())))
                        {
                            if (!string.IsNullOrEmpty(sqlQueryFilter))
                            {
                                sqlQueryFilter = sqlQueryFilter + " WHERE ";
                            }
                            else
                            {
                                sqlQueryFilter = sqlQueryFilter + " and ";
                            }

                            if ((alphaCount > 0) && (item.tables[tableLoop].filters[k].operatorKey.ToString() == "="))
                            {
                                sqlQueryFilter = sqlQueryFilter + " [" + item.tables[tableLoop].schemaNm + "].[" + item.tables[tableLoop].tableNm + "].[" + item.tables[tableLoop].filters[k].columnNm + "] " + item.tables[tableLoop].filters[k].operatorKey + " '" + item.tables[tableLoop].filters[k].value + "' ";
                            }
                            else if (item.tables[tableLoop].filters[k].operatorKey.ToString() == "like")
                            {
                                sqlQueryFilter = sqlQueryFilter + " [" + item.tables[tableLoop].schemaNm + "].[" + item.tables[tableLoop].tableNm + "].[" + item.tables[tableLoop].filters[k].columnNm + "] " + item.tables[tableLoop].filters[k].operatorKey + " '%" + item.tables[tableLoop].filters[k].value + "%' ";
                            }
                            else
                            {
                                sqlQueryFilter = sqlQueryFilter + " [" + item.tables[tableLoop].schemaNm + "].[" + item.tables[tableLoop].tableNm + "].[" + item.tables[tableLoop].filters[k].columnNm + "] " + item.tables[tableLoop].filters[k].operatorKey + " " + item.tables[tableLoop].filters[k].value;
                            }
                        }
                    }

                    //form the query string with aggregate columns for select clause
                    int AggregateArrCount = item.tables[tableLoop].aggregate.Count;
                    for (int l = 0; l < AggregateArrCount; l++)
                    {
                        if (!string.IsNullOrEmpty(sqlQueryAggregateCol))
                        {
                            sqlQueryAggregateCol = sqlQueryAggregateCol + " , ";
                        }                        

                        if ((!string.IsNullOrEmpty(item.tables[tableLoop].aggregate[l].aggregCol.Trim())) && (!string.IsNullOrEmpty(item.tables[tableLoop].aggregate[l].aggregFunc.Trim())))
                        {
                            sqlQueryAggregateCol = sqlQueryAggregateCol + item.tables[tableLoop].aggregate[l].aggregFunc + "([" + item.tables[tableLoop].schemaNm + "].[" + item.tables[tableLoop].tableNm + "].[" + item.tables[tableLoop].aggregate[l].aggregCol + "])";                      
                        }
                    }
                }


                sqlQueryJoin = "[" + item.tables[0].schemaNm + "].[" + item.tables[0].tableNm + "] INNER JOIN [" + item.tables[1].schemaNm + "].[" + item.tables[1].tableNm + "] ON ";

                //loop for getting the join conditions
                for (int i = 0; i < joinArrCount; i++)
                {
                    for (int j = 0; j < item.joins[i].columns.Count; j++)
                    {
                        if (j > 0 || i > 0)
                        {
                            sqlQueryJoin = sqlQueryJoin + " and ";
                        }
                        sqlQueryJoin = sqlQueryJoin + "[" + item.joins[i].table1.name + "].[" + item.joins[i].columns[j].column1 + "] = [" + item.joins[i].table2.name + "].[" + item.joins[i].columns[j].column2 + "]";
                    }
                }

                sqlQuery = " SELECT " + sqlQueryNonAggregCol;
                if (!string.IsNullOrEmpty(sqlQueryAggregateCol))
                {
                    if (!string.IsNullOrEmpty(sqlQueryNonAggregCol))
                    {
                        sqlQuery = sqlQuery + ",";
                    }
                    sqlQuery = sqlQuery + sqlQueryAggregateCol;
                }
                sqlQuery = sqlQuery + " FROM " + sqlQueryJoin + sqlQueryFilter;
                if ((!string.IsNullOrEmpty(sqlQueryAggregateCol)) && (!string.IsNullOrEmpty(sqlQueryNonAggregCol)))
                {
                    sqlQuery = sqlQuery + " GROUP BY " + sqlQueryNonAggregCol;
                }

                con.Open();
                SqlDataAdapter da = new SqlDataAdapter(sqlQuery, con);
                da.Fill(ds);
                con.Close();
                DataTable dtOutput = ds.Tables[0];

                //eliminate the rowguid column from datatable
                for (var i = dtOutput.Columns.Count - 1; i >= 0; i--)
                {
                    if (dtOutput.Columns[i].ColumnName.ToLower().Contains("guid"))
                    {
                        dtOutput.Columns.Remove(dtOutput.Columns[i].ColumnName);
                    }
                }

                //check if the transformation name already exist, if yes return error message          
                string sqlTransformExist = "select * from [AppTable].[tblTransformDetails] where TransformName = '" + item.transformationName.ToString() + "'";
                DataSet dsTransformExist = new DataSet();
                con.Open();
                SqlDataAdapter daTransformExist = new SqlDataAdapter(sqlTransformExist, con);
                daTransformExist.Fill(dsTransformExist);
                con.Close();
                if (dsTransformExist.Tables[0].Rows.Count > 0)
                {
                    return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Transformation already exist, please enter a New Transformation name");
                }

                // save as sql table or azure file
                string outType = item.output.outputType.ToString().ToLower();
                if (outType == "table")
                {
                    if (dtOutput != null)
                    {
                        string sqlTableName = item.output.tableName.ToString();
                        string cmdText = @"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + sqlSchemaName + "' and TABLE_NAME='" + sqlTableName + "') SELECT 1 ELSE SELECT 0";
                        con.Open();
                        SqlCommand cmd = new SqlCommand(cmdText, con);
                        int x = Convert.ToInt32(cmd.ExecuteScalar());
                        if (x == 1)
                            return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Table already exist, please enter a New table name");
                        con.Close();


                        // store the result datatable in SQL server database 
                        Helper.InsertDataIntoSQLServerUsingSQLBulkCopy(dtOutput, sqlTableName, sqlSchemaName);
                    }
                }
                else if (outType == "file")
                {
                    string azureFileName = item.output.fileName.ToString() + ".csv";
                    string userEmail = item.userName.ToString();

                    string[] userArr = userEmail.Split('@');
                    string userName;
                    if (userArr[0].Contains("."))
                    {
                        string[] userNameArr = userArr[0].Split('.');
                        userName = userNameArr[0] + userNameArr[1];
                    }
                    else
                    {
                        userName = userArr[0];
                    }
                    string containerName = userName.ToLower();

                    string storageConnection = Convert.ToString(ConfigurationManager.AppSettings["AzureStorageConnection"]);
                    CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
                    CloudBlobClient blobStorageClient = cloudStorageAccount.CreateCloudBlobClient();
                    //get container reference ,create new if it does not exists
                    var container = blobStorageClient.GetContainerReference(containerName);

                    container.CreateIfNotExists();

                    var blockBlob = container.GetBlockBlobReference(azureFileName);
                    if (blockBlob.Exists())
                    {
                        return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "File already exist, please enter a New file name");
                    }

                    byte[] blobBytes;

                    // read the datatable into memory stream using streamwriter
                    using (var writeStream = new MemoryStream())
                    {
                        using (var swriter = new StreamWriter(writeStream))
                        {
                            //headers  
                            for (int i = 0; i < dtOutput.Columns.Count; i++)
                            {
                                swriter.Write(dtOutput.Columns[i]);
                                if (i < dtOutput.Columns.Count - 1)
                                {
                                    swriter.Write(",");
                                }
                            }
                            swriter.Write(swriter.NewLine);
                            foreach (DataRow dr in dtOutput.Rows)
                            {
                                for (int i = 0; i < dtOutput.Columns.Count; i++)
                                {
                                    if (!Convert.IsDBNull(dr[i]))
                                    {
                                        string value = dr[i].ToString();
                                        if (value.Contains(','))
                                        {
                                            value = String.Format("\"{0}\"", value);
                                            swriter.Write(value);
                                        }
                                        else
                                        {
                                            swriter.Write(dr[i].ToString());
                                        }
                                    }
                                    if (i < dtOutput.Columns.Count - 1)
                                    {
                                        swriter.Write(",");
                                    }
                                }
                                swriter.Write(swriter.NewLine);
                            }
                            swriter.Close();
                        }
                        blobBytes = writeStream.ToArray();
                    }
                    using (var readStream = new MemoryStream(blobBytes))
                    {
                        blockBlob.UploadFromStream(readStream);
                    }
                }

                // insert the meta data information of this transformation in SQL server [AppTable].[tblTransformDetails] table
                string sqlQueryInsert = "INSERT INTO [AppTable].[tblTransformDetails](TransformName,RequestObject,TransformQuery,OutputType,OutputName,SchemaName,UserName,Notes,Flag) VALUES(@TransformName,@RequestObject,@TransformQuery,@OutputType,@OutputName,@SchemaName,@UserName,@Notes,@Flag); SELECT SCOPE_IDENTITY(); ";
                int transformId = 0;
                using (SqlCommand cmd = new SqlCommand(sqlQueryInsert, con))
                {
                    cmd.Parameters.AddWithValue("@TransformName", item.transformationName.ToString());
                    cmd.Parameters.AddWithValue("@RequestObject", strReqBody);
                    cmd.Parameters.AddWithValue("@TransformQuery", sqlQuery);
                    cmd.Parameters.AddWithValue("@OutputType", item.output.outputType);
                    if (outType == "table")
                    {
                        cmd.Parameters.AddWithValue("@OutputName", item.output.tableName.ToString());
                        cmd.Parameters.AddWithValue("@SchemaName", sqlSchemaName);
                    }
                    else if (outType == "file")
                    {
                        cmd.Parameters.AddWithValue("@OutputName", item.output.fileName.ToString());
                        cmd.Parameters.AddWithValue("@SchemaName", string.Empty);
                    }
                    cmd.Parameters.AddWithValue("@UserName", item.userName);
                    cmd.Parameters.AddWithValue("@Notes", item.notes);
                    cmd.Parameters.AddWithValue("@Flag", "I");
                    con.Open();

                    // after insert, store the TransformId auto generated in the variable transformId
                    transformId = Convert.ToInt32(cmd.ExecuteScalar());

                    if (con.State == System.Data.ConnectionState.Open)
                        con.Close();
                }
                drTransformId = dtTransformId.NewRow();
                drTransformId[0] = transformId;
                dtTransformId.Rows.Add(drTransformId);
                dsTransformId.Tables.Add(dtTransformId);

                var jsonResult = JsonConvert.SerializeObject(dsTransformId, Formatting.Indented);
                JObject json = JObject.Parse(jsonResult);
                response = Request.CreateResponse(HttpStatusCode.OK, json);
                return response;
            }
            catch (SqlException exception)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Please give valid values for transformation.");
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }
    }
}
