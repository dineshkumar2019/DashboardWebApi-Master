using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace DashboardWebApi.Controllers
{
    //******* Class for Integration Module *****//
    public class ProductApi
    {
        public string APIURL { get; set; }
        public string APIPostText { get; set; }
    }

    public class APIData
    {
        public int APIID { get; set; }
        public string APIName { get; set; }
        public string APIDescription { get; set; }
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        private string _inputColumns;
        public string InputColumns
        {
            get
            { return _inputColumns; }

            set
            {
                _inputColumns = value;
                string APIPost = "";
                string[] InputColumns = _inputColumns.Split(',');
                if (InputColumns.Length > 0)
                {
                    APIPost = "{";
                    int icount = 0;
                    foreach (string iColumn in InputColumns)
                    {
                        if (icount == InputColumns.Length - 1)
                        { APIPost = APIPost + "\"" + iColumn + "\":\"\" "; }
                        else
                        { APIPost = APIPost + "\"" + iColumn + "\":\"\" ,"; }
                        icount++;
                    }
                    APIPost = APIPost + "}";

                    this.APIPOSTString = APIPost;
                }
            }
        }
        public string OutputColumns { get; set; }
        public string UserName { get; set; }
        public string CreatedDate { get; set; }
        public string APIURL { get; set; }
        public string APIPOSTString { get; set; }
    }

    //******* Class for Transform Module *****//
    //class created for PostSaveQueryTransform
    public class Rootobject
    {
        [JsonProperty("joins")]
        public List<clsJoins> joins { get; set; }
        [JsonProperty("tables")]
        public List<clsTable> tables { get; set; }
        [JsonProperty("output")]
        public clsOutput output { get; set; }
        [JsonProperty("transformationName")]
        public string transformationName { get; set; }
        [JsonProperty("userName")]
        public string userName { get; set; }
        [JsonProperty("notes")]
        public string notes { get; set; }
    }

    public class clsJoins
    {
        [JsonProperty("joinType")]
        public string joinType { get; set; }
        [JsonProperty("table1")]
        public clsTbName_SchemaId table1 { get; set; }
        [JsonProperty("table2")]
        public clsTbName_SchemaId table2 { get; set; }
        [JsonProperty("columns")]
        public List<clsColumns> columns { get; set; }
        [JsonProperty("renderKey")]
        public string renderKey { get; set; }
    }

    public class clsColumns
    {
        [JsonProperty("column1")]
        public string column1 { get; set; }
        [JsonProperty("column2")]
        public string column2 { get; set; }
        [JsonProperty("renderKey")]
        public string renderKey { get; set; }
    }

    public class clsTbName_SchemaId
    {
        [JsonProperty("name")]
        public string name { get; set; }
        [JsonProperty("schemaId")]
        public string schemaId { get; set; }
        [JsonProperty("schemaName")]
        public string schemaName { get; set; }
    }

    public class clsTable
    {
        [JsonProperty("value")]
        public clsTbName_SchemaId value { get; set; }
        [JsonProperty("text")]
        public string text { get; set; }
        [JsonProperty("schemaNm")]
        public string schemaNm { get; set; }
        [JsonProperty("schemaId")]
        public int schemaId { get; set; }
        [JsonProperty("tableNm")]
        public string tableNm { get; set; }
        [JsonProperty("filters")]
        public List<clsFilter> filters { get; set; }
        [JsonProperty("aggregate")]
        public List<clsAggregate> aggregate { get; set; }
        [JsonProperty("renderKey")]
        public string renderKey { get; set; }
        [JsonProperty("columns")]
        public List<string> columns { get; set; }
    }

    public class clsFilter
    {
        [JsonProperty("columnNm")]
        public string columnNm { get; set; }
        [JsonProperty("operatorKey")]
        public string operatorKey { get; set; }
        [JsonProperty("value")]
        public string value { get; set; }
        [JsonProperty("renderKey")]
        public string renderKey { get; set; }

    }

    public class clsAggregate
    {
        [JsonProperty("aggregCol")]
        public string aggregCol { get; set; }
        [JsonProperty("aggregFunc")]
        public string aggregFunc { get; set; }
    }

    public class clsOutput
    {
        [JsonProperty("outputType")]
        public string outputType { get; set; }
        [JsonProperty("fileName")]
        public string fileName { get; set; }
        [JsonProperty("schemaName")]
        public string schemaName { get; set; }
        [JsonProperty("tableName")]
        public string tableName { get; set; }
    }

    //******* Class for Catalog Module *****//
    //class created for GetTableProfile
    public class clsTableProfile
    {
        public clsTableProfile(string ColumnName, string NullCount, string DistinctVal, string MinValue, string MaxValue, string Mean, string StdDev)
        {
            this.ColumnName = ColumnName;
            this.NullCount = NullCount;
            this.DistinctVal = DistinctVal;
            this.MinValue = MinValue;
            this.MaxValue = MaxValue;
            this.Mean = Mean;
            this.StdDev = StdDev;
        }
        [JsonProperty("ColumnName")]
        public string ColumnName { get; set; }
        [JsonProperty("NullCount")]
        public string NullCount { get; set; }
        [JsonProperty("DistinctVal")]
        public string DistinctVal { get; set; }
        [JsonProperty("MinValue")]
        public string MinValue { get; set; }
        [JsonProperty("MaxValue")]
        public string MaxValue { get; set; }
        [JsonProperty("Mean")]
        public string Mean { get; set; }
        [JsonProperty("StdDev")]
        public string StdDev { get; set; }
    }

}