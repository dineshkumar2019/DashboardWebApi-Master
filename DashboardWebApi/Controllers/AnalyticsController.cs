using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Cors;

namespace DashboardWebApi.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    [RoutePrefix("api/Analytics")]
    public class AnalyticsController : ApiController
    {
        /******* Analytics Module *****/
        //Upload csv or txt file to get Linear Regression details. this will get Dependent Variable (output) filed and Independent Variable (Output).
        [HttpPost]
        [Route("LinearRegression/{DependentVariable}/{IndependentVariables}")]
        public async Task<HttpResponseMessage> LinearRegression(string DependentVariable, string IndependentVariables)
        {
            try
            {
                DataTable _sourceTable = new DataTable();

                if (!Request.Content.IsMimeMultipartContent())
                {
                    return Request.CreateResponse(HttpStatusCode.UnsupportedMediaType);
                }
                string fileName = "";
                //string containerName = "blobstutorial"; //ideally should be passed as input parameter
                //var blobAccount = "tesserinsights";
                //var apiKey = "PEgCRIKy9Ko1rbytcZ1JOEfH29UOtMSUJlSOZulXcgpgc3IJ36LITg2Xqr396i7zK0xQsm1ZKehcVhADibwSQQ==";

                //initalize new instance of storage account based on the name and key combination
                //var storageAccount = new CloudStorageAccount(new StorageCredentials(blobAccount, apiKey), true);
                HttpResponseMessage response;

                var filesReadToProvider = await Request.Content.ReadAsMultipartAsync();

                foreach (var stream in filesReadToProvider.Contents)
                {
                    string fileNameStr = stream.Headers.ContentDisposition.FileName;
                    fileNameStr = fileNameStr.Replace('\"', ' ');
                    fileName = fileNameStr.Trim();
                    var fileBytes = await stream.ReadAsByteArrayAsync();
                    var fname = Path.GetFileNameWithoutExtension(fileName);
                    var fext = Path.GetExtension(fileName);

                    if (fext != null)
                    {
                        if (fext.ToString().ToUpper() == ".CSV" || fext.ToString().ToUpper() == ".TXT")
                        {
                            DataTable resultData = new DataTable();
                            resultData = null;
                            resultData = Helper.GetDataTabletFromByteArray(fileBytes);
                            if (resultData == null)
                                return response = Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Please load some data before attempting an analysis");

                            clsLinearRegression cls = new clsLinearRegression();
                            var LR = cls.ProcessLinearRegression(resultData, DependentVariable, IndependentVariables);

                            JObject json = JObject.Parse(LR);
                            return response = Request.CreateResponse(HttpStatusCode.OK, json);

                        }
                        else
                            return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Given File not in .CSV or .TXT Extension");
                    }
                    else
                        return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Unknow File Extension");
                }


                return response = Request.CreateResponse(HttpStatusCode.OK);

            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

        //Upload csv or txt file to get the details in json.
        [HttpPost]
        [Route("FileInfo/{filename}")]
        public async Task<HttpResponseMessage> FileInfo(string filename)
        {
            try
            {
                DataTable _sourceTable = new DataTable();

                if (!Request.Content.IsMimeMultipartContent())
                {
                    return Request.CreateResponse(HttpStatusCode.UnsupportedMediaType);
                }
                string fileName = "";
                HttpResponseMessage response;

                var filesReadToProvider = await Request.Content.ReadAsMultipartAsync();

                foreach (var stream in filesReadToProvider.Contents)
                {
                    string fileNameStr = stream.Headers.ContentDisposition.FileName;
                    fileNameStr = fileNameStr.Replace('\"', ' ');
                    fileName = fileNameStr.Trim();
                    var fileBytes = await stream.ReadAsByteArrayAsync();
                    var fname = Path.GetFileNameWithoutExtension(fileName);
                    var fext = Path.GetExtension(fileName);

                    if (fext != null)
                    {
                        if (fext.ToString().ToUpper() == ".CSV" || fext.ToString().ToUpper() == ".TXT")
                        {
                            DataTable resultData = new DataTable();
                            resultData = null;
                            resultData = Helper.GetDataTabletFromByteArray(fileBytes);
                            if (resultData == null)
                                return response = Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Please load some data before attempting an analysis");
                            response = Request.CreateResponse(HttpStatusCode.OK, resultData, MediaTypeHeaderValue.Parse("application/json"));
                            ContentDispositionHeaderValue contentDisposition = null;
                            if (ContentDispositionHeaderValue.TryParse("inline; filename=ProvantisStudyData.json", out contentDisposition))
                            {
                                response.Content.Headers.ContentDisposition = contentDisposition;
                            }
                            return response;
                            //var jsonResult = JsonConvert.SerializeObject(resultData, Formatting.Indented);
                            //JArray json = JArray.Parse(jsonResult.ToString());
                            ////JObject json = JObject.Parse(jsonResult);
                            //return response = Request.CreateResponse(HttpStatusCode.OK, json);

                        }
                        else
                            return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Given File not in .CSV or .TXT Extension");
                    }
                    else
                        return Request.CreateErrorResponse(HttpStatusCode.BadRequest, "Unknow File Extension");
                }

                return response = Request.CreateResponse(HttpStatusCode.OK);

            }
            catch (Exception ex)
            {
                return Request.CreateErrorResponse(HttpStatusCode.BadRequest, ex.Message);
            }
        }

    }
}
