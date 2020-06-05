using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISMDatabaseUtils;

namespace DMSDatasetRetriever
{
    class DMSDatasetRetriever : PRISM.EventNotifier
    {
        #region "Constants and Enums"

        private enum DatasetInfoColumns
        {
            /// <summary>
            /// Source dataset name
            /// </summary>
            DatasetName = 0,

            /// <summary>
            /// New dataset name
            /// </summary>
            TargetName = 1,

            /// <summary>
            /// Local directory to copy the dataset to
            /// </summary>
            TargetDirectory = 2
        }

        #endregion

        #region "Classwide variables"


        #endregion

        #region "Properties"

        private Dictionary<DatasetInfoColumns, SortedSet<string>> DatasetInfoColumnNames { get; }

        /// <summary>
        /// Retrieval options
        /// </summary>
        public DatasetRetrieverOptions Options { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        public DMSDatasetRetriever(DatasetRetrieverOptions options)
        {
            Options = options;
            DatasetInfoColumnNames = new Dictionary<DatasetInfoColumns, SortedSet<string>>();

            InitializeDatasetInfoFileColumns();
        }

        private void AddDatasetInfoFileColumn(DatasetInfoColumns datasetInfoColumn, params string[] columnNames)
        {
            var columnNameList = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var columnName in columnNames)
            {
                columnNameList.Add(columnName);
            }

            DatasetInfoColumnNames.Add(datasetInfoColumn, columnNameList);
        }

        private string GetColumnValue(
            IReadOnlyDictionary<DatasetInfoColumns, int> columnMapping,
            IReadOnlyList<string> rowData,
            DatasetInfoColumns datasetInfoColumn,
            string valueIfMissing)
        {
            if (!columnMapping.TryGetValue(datasetInfoColumn, out var columnIndex) || columnIndex < 0)
            {
                return valueIfMissing;
            }

            if (columnIndex >= rowData.Count)
            {
                return valueIfMissing;
            }

            return rowData[columnIndex];
        }

        private bool GetDatasetInfoFromDMS(IReadOnlyCollection<DatasetInfo> datasetList)
        {
            try
            {
                var dbTools = DbToolsFactory.GetDBTools(Options.DMSConnectionString);
                RegisterEvents(dbTools);

                // Process 1000 datasets at a time (to prevent the In clause from getting too long)

                // ToDo: change this from 10 to 1000
                const int BATCH_SIZE = 10;

                for (var i = 0; i < datasetList.Count; i += BATCH_SIZE)
                {
                    var datasetBatch = datasetList.Skip(i).Take(BATCH_SIZE);

                    var datasetNameInfoMap = new Dictionary<string, DatasetInfo>();

                    var quotedDatasetNames = new List<string>();
                    foreach (var item in datasetBatch)
                    {
                        datasetNameInfoMap.Add(item.DatasetName, item);
                        quotedDatasetNames.Add("'" + item.DatasetName.Replace("'", "''").Replace(@"\", "") + "'");
                    }

                    var datasetNameList = string.Join(", ", quotedDatasetNames);

                    var sqlQuery =
                        "SELECT DFP.Dataset, DFP.Dataset_ID, DFP.Dataset_Folder_Path, DFP.Archive_Folder_Path, DFP.Instrument_Data_Purged, DE.MyEMSLState " +
                        "FROM V_Dataset_Folder_Paths DFP INNER JOIN  " +
                        "     V_Dataset_Export DE ON DFP.Dataset_ID = DE.ID " +
                        "WHERE DFP.Dataset IN (" + datasetNameList + ")";

                    var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount: 2, callingFunction: "GetDatasetInfoFromDMS");

                    if (!success)
                    {
                        ReportWarning("Error obtaining data from V_Dataset_Folder_Paths for datasets " + GetStartOfString(datasetNameList, 50));
                        return false;
                    }

                    foreach (var dataRow in queryResults)
                    {
                        var datasetName = dataRow[0];
                        var datasetId = dataRow[1];
                        var datasetDirectoryPath = dataRow[2];
                        var archiveDirectoryPath = dataRow[3];
                        var instrumentDataPurged = dataRow[4];
                        var myEmslState = dataRow[5];

                        if (!datasetNameInfoMap.TryGetValue(datasetName, out var datasetInfo))
                        {
                            ReportWarning(string.Format(
                                "Dataset {0} not found in datasetNameInfoMap", datasetName));
                            continue;
                        }

                        if (int.TryParse(datasetId, out var parsedDatasetId))
                        {
                            datasetInfo.DatasetID = parsedDatasetId;
                        }

                        datasetInfo.DatasetDirectoryPath = datasetDirectoryPath;
                        datasetInfo.DatasetArchivePath = archiveDirectoryPath;
                        datasetInfo.InstrumentDataPurged = IntToBool(instrumentDataPurged);
                        datasetInfo.DatasetInMyEMSL = IntToBool(myEmslState);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in GetDatasetInfoFromDMS", ex);
                return false;
            }
        }

        /// <summary>
        /// Get the first n characters from value
        /// Append "..." if the value is more than n characters long
        /// </summary>
        /// <param name="value"></param>
        /// <param name="n"></param>
        /// <returns></returns>
        private string GetStartOfString(string value, int n)
        {
            if (value.Length <= n)
                return value;

            return value.Substring(0, n) + " ...";
        }

        private void InitializeDatasetInfoFileColumns()
        {
            AddDatasetInfoFileColumn(DatasetInfoColumns.DatasetName, "Dataset", "DatasetName", "Dataset Name");

            AddDatasetInfoFileColumn(DatasetInfoColumns.TargetName, "TargetName", "Target Name", "New Name", "DCC_File_Name");

            AddDatasetInfoFileColumn(DatasetInfoColumns.TargetDirectory, "TargetDirectory", "Target Directory", "DCC_Folder_Name");
        }

        /// <summary>
        /// Convert valueText to True or False
        /// </summary>
        /// <param name="valueText"></param>
        /// <returns>True if valueText contains a non-zero integer (positive or negative); otherwise, false</returns>
        private bool IntToBool(string valueText)
        {
            if (int.TryParse(valueText, out var value))
            {
                if (value == 0)
                    return false;

                return true;
            }

            return false;
        }

        private bool LoadDatasetInfoFile(string datasetInfoFilePath, out List<DatasetInfo> datasetList)
        {
            datasetList = new List<DatasetInfo>();

            try
            {
                if (string.IsNullOrWhiteSpace(datasetInfoFilePath))
                {
                    ReportWarning("Dataset info file path is undefined; cannot continue");
                    return false;
                }

                var datasetInfoFile = new FileInfo(datasetInfoFilePath);
                if (!datasetInfoFile.Exists)
                {
                    ReportWarning("Dataset info file not found: " + datasetInfoFile.FullName);
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(datasetInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var columnMapping = new Dictionary<DatasetInfoColumns, int>();

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (columnMapping.Count == 0)
                        {
                            if (!ParseHeaderLine(columnMapping, dataLine, DatasetInfoColumnNames))
                            {
                                return false;
                            }

                            if (columnMapping[DatasetInfoColumns.DatasetName] < 0)
                            {
                                ReportWarning("Dataset info file is missing the Dataset name column; unable to continue");
                                return false;
                            }

                            continue;
                        }

                        var rowData = dataLine.Split('\t').ToList();

                        var datasetName = GetColumnValue(columnMapping, rowData, DatasetInfoColumns.DatasetName, string.Empty);
                        var targetName = GetColumnValue(columnMapping, rowData, DatasetInfoColumns.TargetName, string.Empty);
                        var targetDirectory = GetColumnValue(columnMapping, rowData, DatasetInfoColumns.TargetDirectory, string.Empty);

                        if (string.IsNullOrWhiteSpace(datasetName))
                        {
                            ReportWarning("Skipping line with empty dataset name: " + dataLine);
                        }

                        var datasetInfo = new DatasetInfo(datasetName) {
                            TargetDatasetName = targetName,
                            TargetDirectory = targetDirectory
                        };

                        datasetList.Add(datasetInfo);
                    }

                    if (columnMapping.Count == 0)
                    {
                        ReportWarning("Dataset info file was empty: " + datasetInfoFile.FullName);
                        return false;
                    }
                }

                if (datasetList.Count == 0)
                {
                    ReportWarning("Dataset info file only had a header line: " + datasetInfoFile.FullName);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in LoadDatasetInfoFile", ex);
                return false;
            }
        }

        private bool ParseHeaderLine(
            IDictionary<DatasetInfoColumns, int> columnMapping,
            string dataLine,
            Dictionary<DatasetInfoColumns, SortedSet<string>> datasetInfoColumnNames)
        {
            columnMapping.Clear();
            foreach (var candidateColumn in datasetInfoColumnNames)
            {
                columnMapping.Add(candidateColumn.Key, -1);
            }

            var columnNames = dataLine.Split('\t').ToList();

            if (columnNames.Count < 1)
            {
                ReportWarning("Invalid header line sent to ParseHeaderLine");
                return false;
            }

            var columnIndex = 0;
            var matchFound = false;

            foreach (var columnName in columnNames)
            {
                foreach (var candidateColumn in datasetInfoColumnNames)
                {
                    if (!candidateColumn.Value.Contains(columnName))
                        continue;

                    // Match found
                    columnMapping[candidateColumn.Key] = columnIndex;
                    matchFound = true;
                    break;
                }
                columnIndex++;
            }

            return matchFound;
        }

        private void ReportError(string message)
        {
            OnErrorEvent(message, null);
        }

        private void ReportError(string message, Exception ex)
        {
            OnErrorEvent(message, ex);
        }

        private void ReportWarning(string message)
        {
            OnWarningEvent(message);
        }

        /// <summary>
        /// Read the dataset info file and retrieve the instrument data files for the specified datasets
        /// </summary>
        /// <param name="datasetInfoFilePath"></param>
        /// <param name="outputDirectoryPath"></param>
        /// <returns></returns>
        public bool RetrieveDatasets(string datasetInfoFilePath, string outputDirectoryPath)
        {

            try
            {
                if (string.IsNullOrWhiteSpace(outputDirectoryPath))
                {
                    outputDirectoryPath = ".";
                }

                var datasetInfoLoaded = LoadDatasetInfoFile(datasetInfoFilePath, out var datasetList);
                if (!datasetInfoLoaded)
                    return false;

                var outputDirectory = new DirectoryInfo(outputDirectoryPath);
                if (!outputDirectory.Exists)
                {

                    if (Options.PreviewMode)
                    {
                        Console.WriteLine("Preview create directory: " + outputDirectory.FullName);
                    }
                    else
                    {
                        Console.WriteLine("Creating the output directory: " + outputDirectory.FullName);
                        outputDirectory.Create();
                    }
                }

                var success = RetrieveDatasets(datasetList, outputDirectory);

                return success;
            }
            catch (Exception ex)
            {
                ReportError("Error in RetrieveDatasets", ex);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the instrument data files for the specified datasets
        /// </summary>
        /// <param name="datasetList">Datasets to retrieve</param>
        /// <param name="outputDirectory">Directory where files should be copied</param>
        /// <returns></returns>
        public bool RetrieveDatasets(List<DatasetInfo> datasetList, DirectoryInfo outputDirectory)
        {
            try
            {
                // Obtain metadata from DMS for dataset in datasetList
                var success = GetDatasetInfoFromDMS(datasetList);

                if (!success)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in RetrieveDatasets", ex);
                return false;
            }
        }

        #endregion

    }
}
