using System;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Web;
using Microsoft.VisualBasic;
using System.Text;
using Microsoft.VisualBasic.FileIO;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Configuration;
using System.Collections.Generic;

namespace DashboardWebApi.Controllers
{
    public static class Helper
    {
        private static string CreateTABLE(string tableName, string Schema, DataTable table)
        {
            StringBuilder sqlsc = new StringBuilder(5000);
            //string sqlsc;
            sqlsc.Append("CREATE TABLE [" + Schema + "].[" + tableName + "](");
            for (int i = 0; i < table.Columns.Count; i++)
            {
                sqlsc.Append("\n [" + table.Columns[i].ColumnName + "] ");
                string columnType = table.Columns[i].DataType.ToString();
                switch (columnType)
                {
                    case "System.Int32":
                        sqlsc.Append(" int ");
                        break;
                    case "System.Int64":
                        sqlsc.Append(" bigint ");
                        break;
                    case "System.Int16":
                        sqlsc.Append(" smallint");
                        break;
                    case "System.Byte":
                        sqlsc.Append(" tinyint");
                        break;
                    case "System.Decimal":
                        sqlsc.Append(" decimal ");
                        break;
                    case "System.DateTime":
                        sqlsc.Append(" datetime ");
                        break;
                    case "System.String":
                    default:
                        sqlsc.Append(string.Format(" nvarchar({0}) ", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString()));
                        break;
                }
                if (table.Columns[i].AutoIncrement)
                    sqlsc.Append(" IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ");
                if (!table.Columns[i].AllowDBNull)
                    sqlsc.Append(" NOT NULL ");
                sqlsc.Append(",");
            }
            return sqlsc.ToString().Substring(0, sqlsc.Length - 1) + "\n)";
        }

        // The parameter Schema right now we use SalesLT as default we have to get Schema name from Client application.
        public static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData, string TableName, string SchemaName)
        {
            string connStr = ConfigurationManager.ConnectionStrings["ConnStringSQL"].ConnectionString;
            using (SqlConnection dbConnection = new SqlConnection(connStr))
            {
                dbConnection.Open();
                //string ColumnsDataType = "Varchar(400)";

                string CreateTableStatement = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[" + SchemaName + "].";
                CreateTableStatement += "[" + TableName + "]')";
                CreateTableStatement += " AND type in (N'U'))DROP TABLE [" + SchemaName + "].";
                CreateTableStatement += "[" + TableName + "] ";
                CreateTableStatement += CreateTABLE(TableName, SchemaName, csvFileData);
                SqlCommand CreateTableCmd = new SqlCommand(CreateTableStatement.ToString(), dbConnection);
                CreateTableCmd.ExecuteNonQuery();

                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                {
                    s.BulkCopyTimeout = 0;
                    s.BatchSize = 50;
                    s.DestinationTableName = "[" + SchemaName + "].[" + TableName + "]";
                    s.WriteToServer(csvFileData);
                }
            }
        }

        public static DataTable GetDataTabletFromByteArray(byte[] byteArrayData)
        {
            DataTable csvData = new DataTable();
            try
            {
                BinaryFormatter bformatter = new BinaryFormatter();
                using (MemoryStream stream = new MemoryStream(byteArrayData))
                {
                    //bformatter.Serialize(stream,null);
                    UTF8Encoding encoding = new UTF8Encoding();
                    string str = encoding.GetString(byteArrayData, 0, byteArrayData.Length);
                    StringReader sr = new StringReader(str);
                        using (var csvReader = new TextFieldParser(sr))
                        {
                            csvReader.SetDelimiters(new string[] { ",", "|" });
                            csvReader.HasFieldsEnclosedInQuotes = true;
                            string[] colFields = csvReader.ReadFields();
                            foreach (string column in colFields)
                            {
                                DataColumn datecolumn = new DataColumn(column);
                                datecolumn.AllowDBNull = true;
                                csvData.Columns.Add(datecolumn);
                            }
                            while (!csvReader.EndOfData)
                            {
                                string[] fieldData = csvReader.ReadFields();
                                //Making empty value as null
                                for (int i = 0; i < fieldData.Length; i++)
                                {
                                    if (fieldData[i] == "")
                                    {
                                        fieldData[i] = null;
                                    }
                                }
                                csvData.Rows.Add(fieldData);
                            }
                        }
                    }
                //using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                //{
                //    csvReader.SetDelimiters(new string[] { ",", "|" });
                //    csvReader.HasFieldsEnclosedInQuotes = true;
                //    string[] colFields = csvReader.ReadFields();
                //    foreach (string column in colFields)
                //    {
                //        DataColumn datecolumn = new DataColumn(column);
                //        datecolumn.AllowDBNull = true;
                //        csvData.Columns.Add(datecolumn);
                //    }
                //    while (!csvReader.EndOfData)
                //    {
                //        string[] fieldData = csvReader.ReadFields();
                //        //Making empty value as null
                //        for (int i = 0; i < fieldData.Length; i++)
                //        {
                //            if (fieldData[i] == "")
                //            {
                //                fieldData[i] = null;
                //            }
                //        }
                //        csvData.Rows.Add(fieldData);
                //    }
                //}
            }
            catch (Exception ex)
            {
                return null;
            }
            return csvData;
        }

        //******* Catalog Module *****//
        //Method for table download - read table from sql server and convert to csv and returns List<string>
        public static List<string> ToCSV(this IDataReader dataReader, bool includeHeaderAsFirstRow, string separator)
        {
            List<string> csvRows = new List<string>();
            StringBuilder sb = null;

            if (includeHeaderAsFirstRow)
            {
                sb = new StringBuilder();
                for (int index = 0; index < dataReader.FieldCount; index++)
                {
                    if (dataReader.GetName(index) != null)
                        sb.Append(dataReader.GetName(index));

                    if (index < dataReader.FieldCount - 1)
                        sb.Append(separator);
                }
                csvRows.Add(sb.ToString());
            }

            while (dataReader.Read())
            {
                sb = new StringBuilder();
                for (int index = 0; index < dataReader.FieldCount - 1; index++)
                {
                    if (!dataReader.IsDBNull(index))
                    {
                        string value = dataReader.GetValue(index).ToString();
                        if (dataReader.GetFieldType(index) == typeof(String))
                        {
                            //If double quotes are used in value, ensure each are replaced but 2.
                            if (value.IndexOf("\"") >= 0)
                                value = value.Replace("\"", "\"\"");

                            //If separtor used in value, ensure it is put in double quotes.
                            if (value.IndexOf(separator) >= 0)
                                value = "\"" + value + "\"";
                        }
                        sb.Append(value);
                    }

                    if (index < dataReader.FieldCount - 1)
                        sb.Append(separator);
                }

                if (!dataReader.IsDBNull(dataReader.FieldCount - 1))
                    sb.Append(dataReader.GetValue(dataReader.FieldCount - 1).ToString().Replace(separator, " "));

                csvRows.Add(sb.ToString());
            }
            dataReader.Close();
            sb = null;
            return csvRows;
        }

    }
}