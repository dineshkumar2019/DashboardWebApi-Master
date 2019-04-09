using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Accord.IO;
using Accord.Math;
using Accord.Statistics.Analysis;
using Accord.Statistics.Models.Regression.Linear;
using Accord.Statistics.Testing;
using Newtonsoft.Json;

namespace DashboardWebApi.Controllers
{
    public class LinearCoefficients
    {
        public object Name { get; set; }
        public object Value { get; set; }
        public object StandardError { get; set; }
        public object TStatistic { get; set; }
        public object P_ValueofT { get; set; }
        public object FStatistic { get; set; }
        public object P_ValueofF { get; set; }
        public object ConfidenceUpper { get; set; }
        public object ConfidenceLower { get; set; }
    }
    public class DistributionMesure
    {
        public object Column { get; set; }
        public object Index { get; set; }
        public object Mean { get; set; }
        public object StandardDeviation { get; set; }
        public object Median { get; set; }
        public object Mode { get; set; }
        public object Variance { get; set; }
        public object Max { get; set; }
        public object Min { get; set; }
        public object Length { get; set; }

    }
    public class RegressionAnova
    {
        public object Source { get; set; }
        public object DegreesOfFreedom { get; set; }
        public object SumOfSquares { get; set; }
        public object MeanSquares { get; set; }
        public object FStatistic { get; set; }
        public double? PValueSignificance
        {
            get;
            set;
        }
    }

    public class LinearRegressionOutput
    {
        public List<LinearCoefficients> LinearCoefficientsDataSource { get; set; }
        public List<RegressionAnova> RegressionAnovaDataSource { get; set; }
        public List<DistributionMesure> DistributionMeasuresDataSource { get; set; }
        public string RSquared { get; set; }
        public string RSquaredAdj { get; set; }
        public string ChiPValue { get; set; }
        public string FPValue { get; set; }
        public string ZPValue { get; set; }
        public string ChiStatistic { get; set; }
        public string FStatistic { get; set; }
        public string ZStatistic { get; set; }
        public bool ChiSignificantChecked { get; set; }
        public bool FSignificantChecked { get; set; }
        public bool ZSignificantChecked { get; set; }
        //public DataTable ProjectionSourceDataSource { get; set; }
        public DataTable ProjectionResultDataSource { get; set; }
    }

    public class clsLinearRegression
    {
        private MultipleLinearRegressionAnalysis mlr;
        private LogisticRegressionAnalysis lra;

        public string ProcessLinearRegression(DataTable sourceTable, string _dependentName, string _independentName)
        {
            try
            {

                double[][] inputs;
                double[] outputs;
                LinearRegressionOutput output = new LinearRegressionOutput();

                // Gets the column of the dependent variable
                String dependentName = _dependentName;
                DataTable dependent = sourceTable.DefaultView.ToTable(false, dependentName);

                String[] independentNames = _independentName.Split(',');
                //String[] independentNames = names.ToArray();
                DataTable independent = sourceTable.DefaultView.ToTable(false, independentNames);

                // Creates the input and output matrices from the source data table
                inputs = independent.ToJagged();
                outputs = dependent.Columns[dependentName].ToArray();

                // Creates the Simple Descriptive Analysis of the given source
                var sda = new DescriptiveAnalysis()
                {
                    ColumnNames = independentNames
                }.Learn(inputs);

                // TODO: Standardize the InputNames/OutputNames properties

                // Populates statistics overview tab with analysis data
                DescriptiveMeasureCollection DMC = sda.Measures;

                DistributionMesure dm = new DistributionMesure()
                {
                    //Column = sda.Measures.GetEnumerator(x => x),

                };

                //DMC.s
                //output.DistributionMeasuresDataSource = dm;

                // Creates the Logistic Regression Analysis of the given source
                this.lra = new LogisticRegressionAnalysis()
                {
                    Inputs = independentNames,
                    Output = dependentName
                };

                // Create the Multiple Linear Regression Analysis of the given source
                this.mlr = new MultipleLinearRegressionAnalysis(intercept: true)
                {
                    Inputs = independentNames,
                    Output = dependentName
                };

                // Compute the Linear Regression Analysis
                MultipleLinearRegression reg = mlr.Learn(inputs, outputs);

                LinearRegressionCoefficientCollection LCC = mlr.Coefficients;
                List<LinearCoefficients> lcs = new List<LinearCoefficients>();
                foreach (var rc in LCC)
                {
                    LinearCoefficients lc = new LinearCoefficients()
                    {
                        Name = rc.Name,
                        Value = rc.Value,
                        StandardError = rc.StandardError,
                        TStatistic = rc.TTest.Statistic,
                        P_ValueofT = rc.TTest.PValue,
                        FStatistic = rc.FTest.Statistic,
                        P_ValueofF = rc.FTest.PValue,
                        ConfidenceUpper = rc.ConfidenceUpper,
                        ConfidenceLower = rc.ConfidenceLower
                    };
                    lcs.Add(lc);
                }
                output.LinearCoefficientsDataSource = lcs;

                //AnovaSourceCollection RDS = mlr.Table;
                List<RegressionAnova> acs = new List<RegressionAnova>();
                foreach (var RDS in mlr.Table)
                {
                    RegressionAnova ra = new RegressionAnova()
                    {
                        Source = RDS.Source,
                        DegreesOfFreedom = RDS.DegreesOfFreedom,
                        SumOfSquares = RDS.SumOfSquares,
                        MeanSquares = RDS.MeanSquares,
                        FStatistic = RDS.Statistic,
                        PValueSignificance = (RDS.Significance == null) ? 0 : RDS.Significance.PValue
                    };
                    acs.Add(ra);
                }

                output.RegressionAnovaDataSource = acs;

                output.RSquared = mlr.RSquared.ToString("N5");
                output.RSquaredAdj = mlr.RSquareAdjusted.ToString("N5");
                output.ChiPValue = mlr.ChiSquareTest.PValue.ToString("N5");
                output.FPValue = mlr.FTest.PValue.ToString("N5");
                output.ZPValue = mlr.ZTest.PValue.ToString("N5");
                output.ChiStatistic = mlr.ChiSquareTest.Statistic.ToString("N5");
                output.FStatistic = mlr.FTest.Statistic.ToString("N5");
                output.ZStatistic = mlr.ZTest.Statistic.ToString("N5");
                output.ChiSignificantChecked = mlr.ChiSquareTest.Significant;
                output.FSignificantChecked = mlr.FTest.Significant;
                output.ZSignificantChecked = mlr.ZTest.Significant;

                // Populate projection source table
                string[] cols = independentNames;
                if (!independentNames.Contains(dependentName))
                    cols = independentNames.Concatenate(dependentName);

                DataTable projSource = sourceTable.DefaultView.ToTable(false, cols);
                //output.ProjectionSourceDataSource = projSource;

                DataTable independentProj = projSource.DefaultView.ToTable(false, lra.Inputs);
                DataTable dependentProj = projSource.DefaultView.ToTable(false, lra.Output);

                double[][] input = independentProj.ToJagged();
                double[] output1;
                output1 = mlr.Regression.Transform(input);

                DataTable result = projSource.Clone();
                for (int i = 0; i < input.Length; i++)
                {
                    DataRow row = result.NewRow();
                    for (int j = 0; j < lra.Inputs.Length; j++)
                        row[lra.Inputs[j]] = input[i][j];
                    row[lra.Output] = output1[i];

                    result.Rows.Add(row);
                }

                output.ProjectionResultDataSource = result;

                var jsonResult = JsonConvert.SerializeObject(output, Formatting.Indented);
                return jsonResult;
            }
            catch (Exception ex)
            {
                throw new Exception("Error:" + ex);
            }

        }
    }
}