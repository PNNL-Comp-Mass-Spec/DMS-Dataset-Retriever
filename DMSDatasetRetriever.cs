using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;
using PRISMDatabaseUtils;

namespace DMSDatasetRetriever
{
    class DMSDatasetRetriever : EventNotifier
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

        #region "Properties"

        private Dictionary<DatasetInfoColumns, SortedSet<string>> DatasetInfoColumnNames { get; }

        private Dictionary<string, InstrumentClassInfo> InstrumentClassData { get; }

        /// <summary>
        /// Retrieval options
        /// </summary>
        public DatasetRetrieverOptions Options { get; }


        /// <summary>
        /// List of recent error messages
        /// </summary>
        /// <remarks>Old messages are cleared when ProcessFile is called</remarks>
        // ReSharper disable once CollectionNeverQueried.Global
        public List<string> ErrorMessages { get; }

        /// <summary>
        /// List of recent warning messages
        /// </summary>
        /// <remarks>Old messages are cleared when ProcessFile is called</remarks>
        // ReSharper disable once CollectionNeverQueried.Global
        public List<string> WarningMessages { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        public DMSDatasetRetriever(DatasetRetrieverOptions options)
        {
            Options = options;
            DatasetInfoColumnNames = new Dictionary<DatasetInfoColumns, SortedSet<string>>();
            InstrumentClassData = new Dictionary<string, InstrumentClassInfo>();

            ErrorMessages = new List<string>();
            WarningMessages = new List<string>();

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

        private bool CopyDatasetFiles(IDBTools dbTools, IEnumerable<DatasetInfo> datasetList, DirectoryInfo outputDirectory)
        {
            try
            {
                // Keys in filesToCopy are dataset info objects
                // Values are the list of files (or directories) to copy
                var searchSuccess = FindSourceFiles(dbTools, datasetList, out var sourceFilesByDataset);
                if (!searchSuccess)
                    return false;

                var copySuccess = CopyDatasetFilesToTarget(sourceFilesByDataset, outputDirectory);

                return copySuccess;
            }
            catch (Exception ex)
            {
                ReportError("Error in CopyDatasetFiles", ex);
                return false;
            }
        }

        private bool CopyDatasetFilesToTarget(
            Dictionary<DatasetInfo, List<DatasetFileOrDirectory>> sourceFilesByDataset,
            // ReSharper disable once SuggestBaseTypeForParameter
            DirectoryInfo outputDirectory)
        {
            try
            {
                int debugLevel;
                if (Options.VerboseMode)
                    debugLevel = 2;
                else
                    debugLevel = 1;

                var fileTools = new FileTools("DMSDatasetRetriever", debugLevel);
                RegisterEvents(fileTools);

                foreach (var sourceDataset in sourceFilesByDataset)
                {
                    var datasetInfo = sourceDataset.Key;
                    foreach (var sourceItem in sourceDataset.Value)
                    {
                        if (sourceItem.IsDirectory)
                        {
                            CopyDirectoryToTarget(fileTools, datasetInfo, sourceItem, outputDirectory);
                        }
                        else
                        {
                            CopyFileToTarget(fileTools, datasetInfo, sourceItem, outputDirectory);
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in CopyDatasetFilesToTarget", ex);
                return false;
            }
        }

        private void CopyDirectoryToTarget(
            FileTools fileTools,
            DatasetInfo datasetInfo,
            DatasetFileOrDirectory sourceDirectoryInfo,
            FileSystemInfo outputDirectory)
        {
            try
            {
                var sourceDirectory = new DirectoryInfo(sourceDirectoryInfo.SourcePath);

                if (!sourceDirectory.Exists)
                {
                    ReportWarning("Directory not found, nothing to copy: " + sourceDirectory.FullName);
                    return;
                }

                foreach (var sourceFile in sourceDirectory.GetFiles())
                {
                    // RelativeTargetPath should have the target directory name, possibly preceded by a subdirectory name
                    var relativeTargetPath = Path.Combine(sourceDirectoryInfo.RelativeTargetPath, sourceFile.Name);

                    var sourceFileInfo = new DatasetFileOrDirectory(
                        sourceDirectoryInfo.DatasetInfo,
                        sourceFile,
                        relativeTargetPath,
                        sourceDirectoryInfo.MyEMSLDownloader);

                    CopyFileToTarget(fileTools, datasetInfo, sourceFileInfo, outputDirectory);
                }
            }
            catch (Exception ex)
            {
                ReportError("Error in CreateChecksumFiles", ex);
                return false;
            }
        }

        private void CopyFileToTarget(
            FileTools fileTools,
            DatasetInfo datasetInfo,
            DatasetFileOrDirectory sourceFileInfo,
            FileSystemInfo outputDirectory)
        {
            try
            {
                var sourceFile = new FileInfo(sourceFileInfo.SourcePath);

                // RelativeTargetPath should have the target file name, possibly preceded by a subdirectory name
                var targetFile = new FileInfo(Path.Combine(outputDirectory.FullName, sourceFileInfo.RelativeTargetPath));

                if (!sourceFile.Exists)
                {
                    ReportWarning("File not found, nothing to copy: " + sourceFile.FullName);
                    return;
                }

                if (targetFile.Exists)
                {
                    if (sourceFile.Length == targetFile.Length &&
                        Math.Abs(sourceFile.LastWriteTime.Subtract(targetFile.LastWriteTime).TotalSeconds) < 2.5)
                    {
                        OnDebugEvent("Skipping existing, identical file: " + FileTools.CompactPathString(targetFile.FullName, 60));
                        return;
                    }
                }

                if (Options.PreviewMode)
                {
                    Console.WriteLine("Copy {0} to\n  {1}", sourceFile.FullName, targetFile.FullName);
                }
                else
                {
                    var copySuccess = fileTools.CopyFileUsingLocks(sourceFile, targetFile.FullName, true);
                }

                if (Options.PreviewMode || Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.None)
                    return;

                UpdateChecksumFile(datasetInfo, sourceFile, targetFile, outputDirectory);

            }
            catch (Exception ex)
            {
                ReportError("Error in " + nameof(CopyFileToTarget), ex);
            }
        }

        private void UpdateChecksumFile(DatasetInfo datasetInfo, FileInfo sourceFile, FileInfo targetFile, FileSystemInfo outputDirectory)
        {
            Console.WriteLine("ToDo: compute and/or validate checksums for " + targetFile.FullName);
            Console.WriteLine();

        }

        private bool FindSourceFiles(
            IDBTools dbTools,
            IEnumerable<DatasetInfo> datasetList,
            out Dictionary<DatasetInfo, List<DatasetFileOrDirectory>> sourceFilesByDataset)
        {
            // Keys in this dictionary are dataset info objects
            // Values are the list of files (or directories) to copy
            sourceFilesByDataset = new Dictionary<DatasetInfo, List<DatasetFileOrDirectory>>();

            try
            {

                var myEmslDownloader = new MyEMSLReader.Downloader();

                foreach (var dataset in datasetList)
                {
                    var datasetFileList = new List<DatasetFileOrDirectory>();

                    FileSystemInfo sourceItem;

                    if (string.IsNullOrWhiteSpace(dataset.DatasetFileName))
                    {
                        sourceItem = GetDefaultInstrumentFileOrDirectory(dbTools, dataset);
                        if (sourceItem == null)
                            continue;
                    }
                    else
                    {
                        sourceItem = new FileInfo(dataset.DatasetFileName);
                    }

                    var relativeTargetPath = GetRelativeTargetPath(sourceItem, dataset.TargetDatasetName, dataset.TargetDirectory);

                    if (dataset.InstrumentDataPurged)
                    {
                        if (dataset.DatasetInMyEMSL)
                        {
                            datasetFileList.Add(new DatasetFileOrDirectory(dataset, sourceItem, relativeTargetPath, myEmslDownloader));
                            sourceFilesByDataset.Add(dataset, datasetFileList);
                            continue;
                        }

                        var sourceItemInArchive = Path.Combine(dataset.DatasetArchivePath, sourceItem.Name);
                        datasetFileList.Add(new DatasetFileOrDirectory(dataset, sourceItemInArchive, relativeTargetPath));
                        sourceFilesByDataset.Add(dataset, datasetFileList);
                        continue;
                    }

                    var sourceItemOnStorageServer = Path.Combine(dataset.DatasetDirectoryPath, sourceItem.Name);
                    datasetFileList.Add(new DatasetFileOrDirectory(dataset, sourceItemOnStorageServer, relativeTargetPath));
                    sourceFilesByDataset.Add(dataset, datasetFileList);
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in FindSourceFiles", ex);
                return false;
            }
        }

        private string GetColumnValue(
            IReadOnlyList<string> rowData,
            IReadOnlyDictionary<DatasetInfoColumns, int> columnMap,
            DatasetInfoColumns datasetInfoColumn,
            string valueIfMissing)
        {
            if (!columnMap.TryGetValue(datasetInfoColumn, out var columnIndex) || columnIndex < 0)
            {
                return valueIfMissing;
            }

            if (columnIndex >= rowData.Count)
            {
                return valueIfMissing;
            }

            return rowData[columnIndex];
        }

        private bool GetDatasetInfoFromDMS(IDBTools dbTools, IReadOnlyCollection<DatasetInfo> datasetList)
        {
            try
            {
                // Process 500 datasets at a time to prevent the IN clause from getting too long
                const int BATCH_SIZE = 500;

                for (var i = 0; i < datasetList.Count; i += BATCH_SIZE)
                {
                    var datasetBatch = datasetList.Skip(i).Take(BATCH_SIZE).ToList();

                    // Lookup dataset directory information
                    var success = GetDatasetFolderPathInfo(dbTools, datasetBatch);
                    if (!success)
                        return false;

                    // Now lookup SHA-1 checksums
                    var hashInfoSuccess = GetDatasetFileHashInfo(dbTools, datasetBatch);
                    if (!hashInfoSuccess)
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in GetDatasetInfoFromDMS", ex);
                return false;
            }
        }

        private bool GetDatasetFileHashInfo(IDBTools dbTools, IEnumerable<DatasetInfo> datasetList)
        {
            var datasetIDInfoMap = new Dictionary<int, DatasetInfo>();
            var datasetIDs = new List<int>();
            foreach (var dataset in datasetList)
            {
                if (dataset.DatasetID <= 0)
                    continue;

                datasetIDs.Add(dataset.DatasetID);
                datasetIDInfoMap.Add(dataset.DatasetID, dataset);
            }
            var datasetIdList = string.Join(", ", datasetIDs);

            var columns = new List<string>
                    {
                        "Dataset_ID",
                        "File_Hash",
                        "File_Size_Bytes",
                        "File_Path"
                    };

            var sqlQuery =
                " SELECT " + string.Join(", ", columns) +
                " FROM V_Dataset_Files_List_Report" +
                " WHERE Dataset_ID IN (" + datasetIdList + ")";

            OnDebugEvent(string.Format(
                "Querying {0}, Dataset IDs {1}-{2}",
                "V_Dataset_Files_List_Report", datasetIDs.First(), datasetIDs.Last()));

            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount: 2);

            if (!success)
            {
                ReportWarning("Error obtaining data from V_Dataset_Files_List_Report for dataset IDs " + GetStartOfString(datasetIdList, 50));
                return false;
            }

            var columnMap = dbTools.GetColumnMapping(columns);

            foreach (var resultRow in queryResults)
            {
                var datasetId = dbTools.GetColumnValue(resultRow, columnMap, "Dataset_ID", -1);
                var fileHash = dbTools.GetColumnValue(resultRow, columnMap, "File_Hash");
                var fileSizeBytes = dbTools.GetColumnValue(resultRow, columnMap, "File_Size_Bytes", (long)0);
                var fileNameOrPath = dbTools.GetColumnValue(resultRow, columnMap, "File_Path");

                if (!datasetIDInfoMap.TryGetValue(datasetId, out var datasetInfo))
                {
                    ReportWarning(string.Format(
                        "Dataset ID {0} not found in datasetIDInfoMap", datasetId));
                    continue;
                }

                datasetInfo.DatasetFileName = fileNameOrPath;
                datasetInfo.DatasetFileHashSHA1 = fileHash;
                datasetInfo.DatasetFileSizeBytes = fileSizeBytes;
            }

            return true;
        }

        private bool GetDatasetFolderPathInfo(IDBTools dbTools, IEnumerable<DatasetInfo> datasetList)
        {

            var datasetNameInfoMap = new Dictionary<string, DatasetInfo>(StringComparer.OrdinalIgnoreCase);

            var quotedDatasetNames = new List<string>();

            foreach (var dataset in datasetList)
            {
                if (datasetNameInfoMap.ContainsKey(dataset.DatasetName))
                {
                    ReportWarning("Skipping duplicate dataset " + dataset.DatasetName);
                    continue;
                }

                datasetNameInfoMap.Add(dataset.DatasetName, dataset);
                quotedDatasetNames.Add("'" + dataset.DatasetName.Replace("'", "''").Replace(@"\", "") + "'");
            }

            var datasetNameList = string.Join(", ", quotedDatasetNames);

            var columns = new List<string>
                    {
                        "DFP.Dataset",
                        "DFP.Dataset_ID",
                        "DFP.Dataset_Folder_Path",
                        "DFP.Archive_Folder_Path",
                        "DFP.Instrument_Data_Purged",
                        "DE.MyEMSLState",
                        "InstList.Class AS Instrument_Class"
                    };

            var sqlQuery =
                " SELECT " + string.Join(", ", columns) +
                " FROM V_Dataset_Folder_Paths DFP INNER JOIN" +
                "      V_Dataset_Export DE ON DFP.Dataset_ID = DE.ID INNER JOIN " +
                "      V_Instrument_List_Export InstList ON DE.Instrument = InstList.Name" +
                " WHERE DFP.Dataset IN (" + datasetNameList + ")";

            OnDebugEvent(string.Format(
                "Querying {0}, Dataset {1}",
                "V_Dataset_Folder_Paths", quotedDatasetNames.First().Replace("\'", string.Empty)));

            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount: 2);

            if (!success)
            {
                ReportWarning("Error obtaining data from V_Dataset_Folder_Paths for datasets " + GetStartOfString(datasetNameList, 50));
                return false;
            }

            var columnMap = dbTools.GetColumnMapping(columns);

            foreach (var resultRow in queryResults)
            {
                var datasetName = dbTools.GetColumnValue(resultRow, columnMap, "Dataset");

                if (!datasetNameInfoMap.TryGetValue(datasetName, out var datasetInfo))
                {
                    ReportWarning(string.Format(
                        "Dataset {0} not found in datasetNameInfoMap (this is unexpected)", datasetName));
                    continue;
                }

                var datasetId = dbTools.GetColumnValue(resultRow, columnMap, "Dataset_ID", -1);
                if (datasetId > 0)
                {
                    datasetInfo.DatasetID = datasetId;
                }

                datasetInfo.InstrumentClassName = dbTools.GetColumnValue(resultRow, columnMap, "Instrument_Class");

                datasetInfo.DatasetDirectoryPath = dbTools.GetColumnValue(resultRow, columnMap, "Dataset_Folder_Path");
                datasetInfo.DatasetArchivePath = dbTools.GetColumnValue(resultRow, columnMap, "Archive_Folder_Path");

                var instrumentDataPurged = dbTools.GetColumnValue(resultRow, columnMap, "Instrument_Data_Purged", 0);
                datasetInfo.InstrumentDataPurged = IntToBool(instrumentDataPurged);


                var myEmslState = dbTools.GetColumnValue(resultRow, columnMap, "MyEMSLState", 0);
                datasetInfo.DatasetInMyEMSL = IntToBool(myEmslState);
            }

            foreach (var datasetInfo in datasetNameInfoMap)
            {
                if (datasetInfo.Value.DatasetID <= 0)
                {
                    ReportWarning("Dataset not found in DMS: " + datasetInfo.Value.DatasetName);
                }
            }

            return true;
        }

        private FileSystemInfo GetDefaultInstrumentFileOrDirectory(IDBTools dbTools, DatasetInfo dataset)
        {
            if (InstrumentClassData.Count == 0)
            {
                var classInfoLoaded = LoadInstrumentClassData(dbTools);
                if (!classInfoLoaded)
                {
                    ReportWarning("Unable to load data from V_Instrument_Class_Export; cannot auto-determine the instrument file name");
                    return null;
                }
            }

            if (!InstrumentClassData.TryGetValue(dataset.InstrumentClassName, out var instrumentClassInfo))
            {
                if (dataset.DatasetID > 0)
                {
                    ReportWarning(string.Format(
                        "Skipping dataset due to unrecognized instrument class {0}: {1}",
                        dataset.InstrumentClassName, dataset.DatasetName));
                }

                return null;
            }

            switch (instrumentClassInfo.RawDataType)
            {
                case InstrumentClassInfo.RawDataTypes.DotRawFile:
                    return new FileInfo(dataset.DatasetName + ".raw");

                case InstrumentClassInfo.RawDataTypes.DotRawFolder:
                    return new DirectoryInfo(dataset.DatasetName + ".raw");

                case InstrumentClassInfo.RawDataTypes.DotDFolder:
                    return new DirectoryInfo(dataset.DatasetName + ".d");

                case InstrumentClassInfo.RawDataTypes.DotUimfFile:
                    return new FileInfo(dataset.DatasetName + ".uimf");

                case InstrumentClassInfo.RawDataTypes.BrukerFt:
                case InstrumentClassInfo.RawDataTypes.BrukerTofBaf:
                case InstrumentClassInfo.RawDataTypes.DataFolder:
                    // Unsupported
                    ReportWarning(string.Format(
                        "Skipping dataset due to unsupported RawDataType {0} for instrument class {1}",
                        instrumentClassInfo.RawDataType, instrumentClassInfo.InstrumentClassName));
                    return null;

                default:
                    ReportWarning(string.Format(
                        "Skipping dataset due to unrecognized RawDataType {0} for instrument class {1}",
                        instrumentClassInfo.RawDataType, instrumentClassInfo.InstrumentClassName));
                    return null;
            }
        }

        private string GetRelativeTargetPath(FileSystemInfo sourceItem, string targetDatasetName, string datasetTargetDirectory)
        {
            string relativeTargetPath;
            if (sourceItem is FileInfo sourceFile)
            {
                if (string.IsNullOrWhiteSpace(targetDatasetName))
                {
                    relativeTargetPath = sourceFile.Name;
                }
                else
                {
                    relativeTargetPath = Path.GetFileNameWithoutExtension(targetDatasetName) + sourceFile.Extension;
                }
            }
            else if (sourceItem is DirectoryInfo sourceDirectory)
            {
                if (string.IsNullOrWhiteSpace(targetDatasetName))
                {
                    relativeTargetPath = sourceDirectory.Name;
                }
                else
                {
                    relativeTargetPath = targetDatasetName;
                }
            }
            else
            {
                throw new Exception("Error in GetRelativeTargetPath; source item is not a file or directory: " + sourceItem);
            }

            if (string.IsNullOrWhiteSpace(datasetTargetDirectory))
            {
                return relativeTargetPath;
            }

            return Path.Combine(datasetTargetDirectory, relativeTargetPath);
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
        /// Convert value to True or False
        /// </summary>
        /// <param name="value"></param>
        /// <returns>True if valueText contains a non-zero integer (positive or negative); otherwise, false</returns>
        private bool IntToBool(int value)
        {
            return value != 0;
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
                    var columnMap = new Dictionary<DatasetInfoColumns, int>();

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (columnMap.Count == 0)
                        {
                            var headerLineParsed = DataTableUtils.GetColumnMappingFromHeaderLine(columnMap, dataLine, DatasetInfoColumnNames);
                            if (!headerLineParsed)
                            {
                                return false;
                            }

                            if (columnMap[DatasetInfoColumns.DatasetName] < 0)
                            {
                                ReportWarning("Dataset info file is missing the Dataset name column; unable to continue");
                                return false;
                            }

                            continue;
                        }

                        var rowData = dataLine.Split('\t').ToList();

                        var datasetName = GetColumnValue(rowData, columnMap, DatasetInfoColumns.DatasetName, string.Empty);
                        var targetName = GetColumnValue(rowData, columnMap, DatasetInfoColumns.TargetName, string.Empty);
                        var targetDirectory = GetColumnValue(rowData, columnMap, DatasetInfoColumns.TargetDirectory, string.Empty);

                        if (string.IsNullOrWhiteSpace(datasetName))
                        {
                            ReportWarning("Skipping line with empty dataset name: " + dataLine);
                        }

                        var datasetInfo = new DatasetInfo(datasetName)
                        {
                            TargetDatasetName = targetName,
                            TargetDirectory = targetDirectory
                        };

                        datasetList.Add(datasetInfo);
                    }

                    if (columnMap.Count == 0)
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

        private bool LoadInstrumentClassData(IDBTools dbTools)
        {

            try
            {

                var columns = new List<string>
                    {
                        "Instrument_Class",
                        "Is_Purgable",
                        "Raw_Data_Type",
                        "Comment"
                    };

                var sqlQuery =
                    " SELECT " + string.Join(", ", columns) +
                    " FROM V_Instrument_Class_Export";

                OnDebugEvent("Querying V_Instrument_Class_Export");

                var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount: 2);

                if (!success)
                {
                    ReportWarning("Error obtaining data from V_Instrument_Class_Export");
                    return false;
                }

                var columnMap = dbTools.GetColumnMapping(columns);

                foreach (var resultRow in queryResults)
                {
                    var instrumentClassName = dbTools.GetColumnValue(resultRow, columnMap, "Instrument_Class");
                    var isPurgable = dbTools.GetColumnValue(resultRow, columnMap, "Is_Purgable", 0);
                    var rawDataTypeName = dbTools.GetColumnValue(resultRow, columnMap, "Raw_Data_Type");
                    var comment = dbTools.GetColumnValue(resultRow, columnMap, "Comment");

                    var instrumentClassInfo = new InstrumentClassInfo(instrumentClassName, rawDataTypeName, IntToBool(isPurgable), comment);

                    InstrumentClassData[instrumentClassName] = instrumentClassInfo;
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in " + nameof(LoadInstrumentClassData), ex);
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
            ReportError(message, null);
            ErrorMessages.Add(message);
        }

        private void ReportError(string message, Exception ex)
        {
            OnErrorEvent(message, ex);
            ErrorMessages.Add(message);
        }

        private void ReportWarning(string message)
        {
            OnWarningEvent(message);
            WarningMessages.Add(message);
        }
        /// <summary>
        /// Read the dataset info file and retrieve the instrument data files for the specified datasets
        /// </summary>
        /// <param name="datasetInfoFilePath"></param>
        /// <param name="outputDirectoryPath"></param>
        /// <returns></returns>
        public bool RetrieveDatasetFiles(string datasetInfoFilePath, string outputDirectoryPath)
        {

            try
            {
                ErrorMessages.Clear();
                WarningMessages.Clear();

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

                var success = RetrieveDatasetFiles(datasetList, outputDirectory, false);

                ShowCachedMessages();

                return success;
            }
            catch (Exception ex)
            {
                ReportError("Error in RetrieveDatasetFiles (datasetInfoFile)", ex);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the instrument data files for the specified datasets
        /// </summary>
        /// <param name="datasetList">Datasets to retrieve</param>
        /// <param name="outputDirectory">Directory where files should be copied</param>
        /// <returns></returns>
        // ReSharper disable once UnusedMember.Global
        public bool RetrieveDatasetFiles(List<DatasetInfo> datasetList, DirectoryInfo outputDirectory)
        {
            var success = RetrieveDatasetFiles(datasetList, outputDirectory, true);

            ShowCachedMessages();

            return success;
        }

        /// <summary>
        /// Retrieve the instrument data files for the specified datasets
        /// </summary>
        /// <param name="datasetList">Datasets to retrieve</param>
        /// <param name="outputDirectory">Directory where files should be copied</param>
        /// <param name="clearCachedMessages">When true, clear ErrorMessages and WarningMessages</param>
        /// <returns></returns>
        private bool RetrieveDatasetFiles(List<DatasetInfo> datasetList, DirectoryInfo outputDirectory, bool clearCachedMessages)
        {
            try
            {
                if (clearCachedMessages)
                {
                    ErrorMessages.Clear();
                    WarningMessages.Clear();
                }

                if (datasetList == null)
                {
                    ReportWarning(string.Format(
                        "Null value for {0} provided to RetrieveDatasetFiles; cannot continue", nameof(datasetList)));
                }

                if (outputDirectory == null)
                {
                    ReportWarning(string.Format(
                        "Null value for {0} provided to RetrieveDatasetFiles; cannot continue", nameof(outputDirectory)));
                }

                var dbTools = DbToolsFactory.GetDBTools(Options.DMSConnectionString);
                RegisterEvents(dbTools);

                // Obtain metadata from DMS for dataset in datasetList
                var success = GetDatasetInfoFromDMS(dbTools, datasetList);

                if (!success)
                    return false;

                var copyFileSuccess = CopyDatasetFiles(dbTools, datasetList, outputDirectory);
                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in RetrieveDatasetFiles (datasetList)", ex);
                return false;
            }
        }

        private void ShowCachedMessages()
        {
            if (WarningMessages.Count == 0 && ErrorMessages.Count == 0)
                return;

            Console.WriteLine();
            Console.WriteLine();

            var headerLine = "** Problems encountered during processing **";
            Console.WriteLine(new string('*', headerLine.Length));
            Console.WriteLine(headerLine);
            Console.WriteLine(new string('*', headerLine.Length));

            foreach (var message in WarningMessages)
            {
                ConsoleMsgUtils.ShowWarning(message);
            }

            foreach (var message in ErrorMessages)
            {
                ConsoleMsgUtils.ShowError(message);
            }
        }

        #endregion

        #region "Event Handlers"

        private void FileCopyUtilityOnProgressUpdate(string progressMessage, float percentComplete)
        {
            var percentCompleteOverall = percentComplete * 0.5;
            ReportProgress(progressMessage, percentComplete, percentCompleteOverall);
        }

        private void FileHashUtilityOnProgressUpdate(string progressMessage, float percentComplete)
        {
            var percentCompleteOverall = 0.5 + percentComplete * 0.5;
            ReportProgress(progressMessage, percentComplete, percentCompleteOverall);
        }

        #endregion
    }


}
