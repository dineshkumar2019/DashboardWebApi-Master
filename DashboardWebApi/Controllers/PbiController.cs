using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Cors;
//using Microsoft.AspNetCore.Http;
//using Microsoft.AspNetCore.Mvc;
using Microsoft.PowerBI.Api.V2;
using Microsoft.PowerBI.Api.V2.Models;
using Microsoft.Rest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Visualize.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    [RoutePrefix("api/Pbi")]
    public class PbiController : ApiController
    {

        [HttpGet]
        [Route("GetWorkSpaces")]
        public HttpResponseMessage GetWorkSpaces()
        {
            try
            {
                HttpResponseMessage response;
                var accessToken = ((string[])Request.Headers.GetValues("Authorization"))[0];
                using (var client = new PowerBIClient(new Uri("https://api.powerbi.com"), new TokenCredentials(accessToken, "Bearer")))
                {
                    var workspaces = client.Groups.GetGroups();
                    var jsonResult = JsonConvert.SerializeObject(workspaces.Value.ToList(), Formatting.Indented);
                    //JObject json = JObject.Parse(jsonResult);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonResult);
                    return response;
                    //return workspaces.Value.ToList();
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        [HttpGet]
        [Route("GetReports/{workspaceId}")]
        public HttpResponseMessage GetReports(string workspaceId)
        {
            try
            {
                HttpResponseMessage response;
                var accessToken = ((string[])Request.Headers.GetValues("Authorization"))[0];//Request.Headers.GetValues("Authorization").ToString();
                using (var client = new PowerBIClient(new Uri("https://api.powerbi.com"), new TokenCredentials(accessToken, "Bearer")))
                {
                    var reports = client.Reports.GetReports(workspaceId);
                    var jsonResult = JsonConvert.SerializeObject(reports.Value.ToList(), Formatting.Indented);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonResult);
                    return response;
                    //return reports.Value.ToList();
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        [HttpGet]
        [Route("GetReport/{workspaceId}/{reportId}")]
        public HttpResponseMessage GetReport(string workspaceId, string reportId)
        {
            try
            {
                HttpResponseMessage response;
                var accessToken = ((string[])Request.Headers.GetValues("Authorization"))[0];//Request.Headers.GetValues("Authorization").ToString();
                using (var client = new PowerBIClient(new Uri("https://api.powerbi.com"), new TokenCredentials(accessToken, "Bearer")))
                {
                    var report = client.Reports.GetReportInGroup(workspaceId, reportId);
                    //var report = client.Reports.GetReport(reportId);
                    Dictionary<string, string> reportDetails = new Dictionary<string, string>();
                    reportDetails.Add("reportId", report.Id);
                    reportDetails.Add("embedUrl", report.EmbedUrl);
                   // return reportDetails;

                    var jsonResult = JsonConvert.SerializeObject(reportDetails, Formatting.Indented);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonResult);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        [HttpGet]
        [Route("GetDatasets/{workspaceId}")]
        public HttpResponseMessage GetDatasets(string workspaceId)
        {
            try
            {
                HttpResponseMessage response;
                var accessToken = ((string[])Request.Headers.GetValues("Authorization"))[0];//Request.Headers.GetValues("Authorization").ToString();
                using (var client = new PowerBIClient(new Uri("https://api.powerbi.com"), new TokenCredentials(accessToken, "Bearer")))
                {
                    var datasets = client.Datasets.GetDatasetsInGroup(workspaceId);
                    //return datasets.Value.ToList();
                    var jsonResult = JsonConvert.SerializeObject(datasets.Value.ToList(), Formatting.Indented);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonResult);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        [HttpGet]
        [Route("GetAccessTokenForCreate/{workspaceId}/{datasetId}")]
        public HttpResponseMessage GetAccessTokenForCreate(string workspaceId, string datasetId)
        {
            try
            {
                HttpResponseMessage response;
                var accessToken = ((string[])Request.Headers.GetValues("Authorization"))[0];//Request.Headers.GetValues("Authorization").ToString();
                using (var client = new PowerBIClient(new Uri("https://api.powerbi.com"), new TokenCredentials(accessToken, "Bearer")))
                {
                    GenerateTokenRequest generateTokenRequestParameters = new GenerateTokenRequest(accessLevel: TokenAccessLevel.Create, datasetId: datasetId, allowSaveAs: true);
                    EmbedToken tokenResponse = client.Reports.GenerateTokenForCreateInGroup(workspaceId, generateTokenRequestParameters);
                    //return tokenResponse;
                    var jsonResult = JsonConvert.SerializeObject(tokenResponse, Formatting.Indented);
                    response = Request.CreateResponse(HttpStatusCode.OK, jsonResult);
                    return response;
                }
            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }


        //[HttpPost]
        //[Route("CreateDataset")]
        //public void CreateDataset()
        //{
        //    string datasetId;
        //    //TODO: Add using System.Net and using System.IO
        //    var token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik4tbEMwbi05REFMcXdodUhZbkhRNjNHZUNYYyIsImtpZCI6Ik4tbEMwbi05REFMcXdodUhZbkhRNjNHZUNYYyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvMWU4ZjE5YTctYjkxNi00YzNlLWEwOGEtOGFjYjQ2ZDRlYzdjLyIsImlhdCI6MTU1NDcxMTgyMywibmJmIjoxNTU0NzExODIzLCJleHAiOjE1NTQ3MTU3MjMsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVFFBeS84TEFBQUFTMjl2MjUyYW1paHlDUzFGZFVCSm92cERGS3JrS0xBdHVqSkVabnpCaVplbnpvMnpqbkZkdndCa2hSTk1TQ2pxIiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6ImM2NGUwZTg4LWZlYzktNGUzNC1hNTY4LTIxN2Q1N2FhYTRkZCIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiVmFyYWRhcmFqYW4iLCJnaXZlbl9uYW1lIjoiSGFyaW5pIiwiaXBhZGRyIjoiMTIyLjE3NC4xMDkuOCIsIm5hbWUiOiJIYXJpbmkgVmFyYWRhcmFqYW4iLCJvaWQiOiIzZThkZGU4NS03MjBjLTQ5YjEtYWRmMy1jM2VhMWQ1YjEwMTMiLCJwdWlkIjoiMTAwMzIwMDAzQUM2MDNGNiIsInNjcCI6IkFwcC5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkV3JpdGUuQWxsIENvbnRlbnQuQ3JlYXRlIERhc2hib2FyZC5SZWFkLkFsbCBEYXNoYm9hcmQuUmVhZFdyaXRlLkFsbCBEYXRhLkFsdGVyX0FueSBEYXRhZmxvdy5SZWFkLkFsbCBEYXRhZmxvdy5SZWFkV3JpdGUuQWxsIERhdGFzZXQuUmVhZC5BbGwgRGF0YXNldC5SZWFkV3JpdGUuQWxsIEdhdGV3YXkuUmVhZC5BbGwgR2F0ZXdheS5SZWFkV3JpdGUuQWxsIEdyb3VwLlJlYWQgR3JvdXAuUmVhZC5BbGwgTWV0YWRhdGEuVmlld19BbnkgUmVwb3J0LlJlYWQuQWxsIFJlcG9ydC5SZWFkV3JpdGUuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWQuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWRXcml0ZS5BbGwgVGVuYW50LlJlYWQuQWxsIFRlbmFudC5SZWFkV3JpdGUuQWxsIFdvcmtzcGFjZS5SZWFkLkFsbCBXb3Jrc3BhY2UuUmVhZFdyaXRlLkFsbCIsInN1YiI6Ik5kT2YzeFVYSGxMMmZxMzRKU1NkcmN2NmROd2tKNzByMFZsSEx5bktKZlUiLCJ0aWQiOiIxZThmMTlhNy1iOTE2LTRjM2UtYTA4YS04YWNiNDZkNGVjN2MiLCJ1bmlxdWVfbmFtZSI6ImhhcmluaS52YXJhZGFyYWphbkBmaXZlcG9pbnRmaXZlc29sdXRpb25zLmNvbSIsInVwbiI6ImhhcmluaS52YXJhZGFyYWphbkBmaXZlcG9pbnRmaXZlc29sdXRpb25zLmNvbSIsInV0aSI6IjNpVTZRVEdlQmtPS0hZdnlqQkU5QUEiLCJ2ZXIiOiIxLjAifQ.Gg9JTbl4a7Fe4hlKjOYLXaDL3JTRgRIvYtvO-1b15-shRPpH4DYbIZX1VMfthK0qsaxUwY1_p4ClXHKYQKchlAgu8sPMSKqX8UpmGK1MI453j20eLqBdVone_b52iyhxFeLJX94GUwANymIyNYDXD5Bt2uAv3yhfv6GG01bW71WjO6EK9Qo-701VO6JWcasvjguQjHFLFwOTPx3IDZWMLxyESAARsNgGrIal-ZG7n5-jsJeU60kobFOWq9ULp4AwGXdbx8Oh1aRVXZeE2boPO1jQKSc4FIfuLDp4AQDTElYj_rwz8hBzBuwH8wWlHR5GT89-xPWXv0zf8_7HLkRXjg";
        //    //string powerBIDatasetsApiUrl = "https://api.powerbi.com/v1.0/myorg/datasets";
        //    string powerBIDatasetsApiUrl = "https://api.PowerBI.com/v1.0/myorg/groups/cdfed04d-19cd-434d-bfd1-1cb1641e94de/datasets";
        //    //POST web request to create a dataset.
        //    //To create a Dataset in a group, use the Groups uri: https://api.PowerBI.com/v1.0/myorg/groups/{group_id}/datasets
        //    HttpWebRequest request = System.Net.WebRequest.Create(powerBIDatasetsApiUrl) as System.Net.HttpWebRequest;
        //    request.KeepAlive = true;
        //    request.Method = "POST";
        //    request.ContentLength = 0;
        //    request.ContentType = "application/json";

        //    //Add token to the request header
        //    request.Headers.Add("Authorization", String.Format("Bearer {0}", token));

        //    //Create dataset JSON for POST request
        //    string datasetJson = "{\"name\": \"SalesMarketing4\", \"tables\": " +
        //        "[{\"name\": \"Product4\", \"columns\": " +
        //        "[{ \"name\": \"ProductID\", \"dataType\": \"Int64\"}, " +
        //        "{ \"name\": \"Name\", \"dataType\": \"string\"}, " +
        //        "{ \"name\": \"Category\", \"dataType\": \"string\"}," +
        //        "{ \"name\": \"IsCompete\", \"dataType\": \"bool\"}," +
        //        "{ \"name\": \"ManufacturedOn\", \"dataType\": \"DateTime\"}" +
        //        "]}]}";

        //    //POST web request
        //    byte[] byteArray = System.Text.Encoding.UTF8.GetBytes(datasetJson);
        //    request.ContentLength = byteArray.Length;

        //    //Write JSON byte[] into a Stream
        //    using (Stream writer = request.GetRequestStream())
        //    {
        //        writer.Write(byteArray, 0, byteArray.Length);

        //        // var response = (HttpWebResponse)request.GetResponse();
        //        using (HttpWebResponse httpResponse = request.GetResponse() as System.Net.HttpWebResponse)
        //        {
        //            //Get StreamReader that holds the response stream
        //            using (StreamReader reader = new System.IO.StreamReader(httpResponse.GetResponseStream()))
        //            {
        //                string responseContent = reader.ReadToEnd();

        //                //TODO: Install NuGet Newtonsoft.Json package: Install-Package Newtonsoft.Json
        //                //and add using Newtonsoft.Json
        //                var results = JsonConvert.DeserializeObject<dynamic>(responseContent);

        //                //Get the first id
        //                datasetId = results.id;

        //                AddRows(datasetId, "Product4");
        //            }
        //        }

        //    }

           

        //}


        //[HttpPost]
        //[Route("AddRows")]
        //public void AddRows(string datasetId, string tableName)
        //{
        //    var token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik4tbEMwbi05REFMcXdodUhZbkhRNjNHZUNYYyIsImtpZCI6Ik4tbEMwbi05REFMcXdodUhZbkhRNjNHZUNYYyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvMWU4ZjE5YTctYjkxNi00YzNlLWEwOGEtOGFjYjQ2ZDRlYzdjLyIsImlhdCI6MTU1NDcxMTgyMywibmJmIjoxNTU0NzExODIzLCJleHAiOjE1NTQ3MTU3MjMsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVFFBeS84TEFBQUFTMjl2MjUyYW1paHlDUzFGZFVCSm92cERGS3JrS0xBdHVqSkVabnpCaVplbnpvMnpqbkZkdndCa2hSTk1TQ2pxIiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6ImM2NGUwZTg4LWZlYzktNGUzNC1hNTY4LTIxN2Q1N2FhYTRkZCIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiVmFyYWRhcmFqYW4iLCJnaXZlbl9uYW1lIjoiSGFyaW5pIiwiaXBhZGRyIjoiMTIyLjE3NC4xMDkuOCIsIm5hbWUiOiJIYXJpbmkgVmFyYWRhcmFqYW4iLCJvaWQiOiIzZThkZGU4NS03MjBjLTQ5YjEtYWRmMy1jM2VhMWQ1YjEwMTMiLCJwdWlkIjoiMTAwMzIwMDAzQUM2MDNGNiIsInNjcCI6IkFwcC5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkLkFsbCBDYXBhY2l0eS5SZWFkV3JpdGUuQWxsIENvbnRlbnQuQ3JlYXRlIERhc2hib2FyZC5SZWFkLkFsbCBEYXNoYm9hcmQuUmVhZFdyaXRlLkFsbCBEYXRhLkFsdGVyX0FueSBEYXRhZmxvdy5SZWFkLkFsbCBEYXRhZmxvdy5SZWFkV3JpdGUuQWxsIERhdGFzZXQuUmVhZC5BbGwgRGF0YXNldC5SZWFkV3JpdGUuQWxsIEdhdGV3YXkuUmVhZC5BbGwgR2F0ZXdheS5SZWFkV3JpdGUuQWxsIEdyb3VwLlJlYWQgR3JvdXAuUmVhZC5BbGwgTWV0YWRhdGEuVmlld19BbnkgUmVwb3J0LlJlYWQuQWxsIFJlcG9ydC5SZWFkV3JpdGUuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWQuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWRXcml0ZS5BbGwgVGVuYW50LlJlYWQuQWxsIFRlbmFudC5SZWFkV3JpdGUuQWxsIFdvcmtzcGFjZS5SZWFkLkFsbCBXb3Jrc3BhY2UuUmVhZFdyaXRlLkFsbCIsInN1YiI6Ik5kT2YzeFVYSGxMMmZxMzRKU1NkcmN2NmROd2tKNzByMFZsSEx5bktKZlUiLCJ0aWQiOiIxZThmMTlhNy1iOTE2LTRjM2UtYTA4YS04YWNiNDZkNGVjN2MiLCJ1bmlxdWVfbmFtZSI6ImhhcmluaS52YXJhZGFyYWphbkBmaXZlcG9pbnRmaXZlc29sdXRpb25zLmNvbSIsInVwbiI6ImhhcmluaS52YXJhZGFyYWphbkBmaXZlcG9pbnRmaXZlc29sdXRpb25zLmNvbSIsInV0aSI6IjNpVTZRVEdlQmtPS0hZdnlqQkU5QUEiLCJ2ZXIiOiIxLjAifQ.Gg9JTbl4a7Fe4hlKjOYLXaDL3JTRgRIvYtvO-1b15-shRPpH4DYbIZX1VMfthK0qsaxUwY1_p4ClXHKYQKchlAgu8sPMSKqX8UpmGK1MI453j20eLqBdVone_b52iyhxFeLJX94GUwANymIyNYDXD5Bt2uAv3yhfv6GG01bW71WjO6EK9Qo-701VO6JWcasvjguQjHFLFwOTPx3IDZWMLxyESAARsNgGrIal-ZG7n5-jsJeU60kobFOWq9ULp4AwGXdbx8Oh1aRVXZeE2boPO1jQKSc4FIfuLDp4AQDTElYj_rwz8hBzBuwH8wWlHR5GT89-xPWXv0zf8_7HLkRXjg";

        //    //string powerBIApiAddRowsUrl = String.Format("https://api.powerbi.com/v1.0/myorg/datasets/{0}/tables/{1}/rows", datasetId, tableName);
        //    string powerBIApiAddRowsUrl = String.Format("https://api.powerbi.com/v1.0/myorg/groups/cdfed04d-19cd-434d-bfd1-1cb1641e94de/datasets/{0}/tables/{1}/rows", datasetId, tableName);

        //    //POST web request to add rows.
        //    //To add rows to a dataset in a group, use the Groups uri: https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/tables/{table_name}/rows
        //    //Change request method to "POST"
        //    HttpWebRequest request = System.Net.WebRequest.Create(powerBIApiAddRowsUrl) as System.Net.HttpWebRequest;
        //    request.KeepAlive = true;
        //    request.Method = "POST";
        //    request.ContentLength = 0;
        //    request.ContentType = "application/json";

        //    //Add token to the request header
        //    request.Headers.Add("Authorization", String.Format("Bearer {0}", token));

        //    //JSON content for product row
        //    string rowsJson = "{\"rows\":" +
        //        "[{\"ProductID\":1,\"Name\":\"Adjustable Race\",\"Category\":\"Components\",\"IsCompete\":true,\"ManufacturedOn\":\"07/30/2014\"}," +
        //        "{\"ProductID\":2,\"Name\":\"LL Crankarm\",\"Category\":\"Components\",\"IsCompete\":true,\"ManufacturedOn\":\"07/30/2014\"}," +
        //        "{\"ProductID\":3,\"Name\":\"HL Mountain Frame - Silver\",\"Category\":\"Bikes\",\"IsCompete\":true,\"ManufacturedOn\":\"07/30/2014\"}]}";

        //    //POST web request
        //    byte[] byteArray = System.Text.Encoding.UTF8.GetBytes(rowsJson);
        //    request.ContentLength = byteArray.Length;

        //    //Write JSON byte[] into a Stream
        //    using (Stream writer = request.GetRequestStream())
        //    {
        //        writer.Write(byteArray, 0, byteArray.Length);

        //        var response = (HttpWebResponse)request.GetResponse();

        //        //Console.WriteLine("Rows Added");

        //        //Console.ReadLine();
        //    }
        //}


    }
}