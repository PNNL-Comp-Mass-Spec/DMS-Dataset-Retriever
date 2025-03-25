using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;
using PRISM.FileProcessor;
using PRISMDatabaseUtils;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// Find files for the specified datasets and copy them to the output directory
    /// Create checksum files listing SHA-1 and/or MD5 hashes for each file
    /// </summary>
    public class DMSDatasetRetriever : EventNotifier
    {
        // Ignore Spelling: purgeable

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

            DataTableUtils.GetColumnIndexAllowColumnNameMatchOnly = true;
            DataTableUtils.GetColumnIndexAllowFuzzyMatch = true;
            DataTableUtils.GetColumnValueThrowExceptions = false;

            InitializeDatasetInfoFileColumns();
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

                return CopyDatasetFilesToTarget(sourceFilesByDataset, outputDirectory);
            }
            catch (Exception ex)
            {
                ReportError("Error in CopyDatasetFiles", ex);
                return false;
            }
        }

        private bool CopyDatasetFilesToTarget(
            Dictionary<DatasetInfo, List<DatasetFileOrDirectory>> sourceFilesByDataset,
            DirectoryInfo outputDirectory)
        {
            try
            {
                var fileCopyUtility = new FileCopyUtility(Options);
                RegisterEvents(fileCopyUtility);

                fileCopyUtility.ProgressUpdate += FileCopyUtilityOnProgressUpdate;
                fileCopyUtility.SkipConsoleWriteIfNoProgressListener = true;

                return fileCopyUtility.CopyDatasetFilesToTarget(sourceFilesByDataset, outputDirectory);
            }
            catch (Exception ex)
            {
                ReportError("Error in CopyDatasetFilesToTarget", ex);
                return false;
            }
        }

        /// <summary>
        /// Create one or more checksum files (depending on Options.ChecksumFileMode)
        /// </summary>
        /// <param name="datasetList"></param>
        /// <param name="outputDirectory"></param>
        private bool CreateChecksumFiles(IEnumerable<DatasetInfo> datasetList, FileSystemInfo outputDirectory)
        {
            try
            {
                if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.None)
                {
                    OnWarningEvent("DMSDatasetRetriever.CreateChecksumFiles called when Options.ChecksumFileMode is ChecksumFileType.None; nothing to do");
                    return true;
                }

                var fileHashUtility = new FileHashUtility(Options);
                RegisterEvents(fileHashUtility);

                fileHashUtility.ProgressUpdate += FileHashUtilityOnProgressUpdate;
                fileHashUtility.SkipConsoleWriteIfNoProgressListener = true;

                return fileHashUtility.CreateChecksumFiles(datasetList, outputDirectory.FullName);
            }
            catch (Exception ex)
            {
                ReportError("Error in CreateChecksumFiles", ex);
                return false;
            }
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

        /// <summary>
        /// Create a string showing the item count followed by the units, which will differ depending on if the count is 1 or otherwise
        /// </summary>
        /// <param name="itemCount">Item count</param>
        /// <param name="unitsIfSingle">Units if just one item</param>
        /// <param name="unitsIfMultiOrZero">Units if zero items or multiple items</param>
        public static string GetCountWithUnits(int itemCount, string unitsIfSingle, string unitsIfMultiOrZero)
        {
            return string.Format("{0} {1}", itemCount, itemCount == 1 ? unitsIfSingle : unitsIfMultiOrZero);
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

        private bool GetDatasetFileHashInfo(IDBTools dbTools, IReadOnlyCollection<DatasetInfo> datasetList)
        {
            if (datasetList.Count == 0)
            {
                ReportWarning("Error: Empty dataset list passed to GetDatasetFileHashInfo");
                return false;
            }

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

            if (datasetIDs.Count == 0)
            {
                ReportWarning("Error: None of the items passed to GetDatasetFileHashInfo in datasetList has a non-zero Dataset ID");
                return false;
            }

            var columnNames = new List<string>
                    {
                        "dataset_id",
                        "file_hash",
                        "file_size_bytes",
                        "file_path"
                    };

            var sqlQuery =
                " SELECT " + string.Join(", ", columnNames) +
                " FROM V_Dataset_Files_List_Report" +
                " WHERE dataset_id IN (" + datasetIdList + ")";

            OnDebugEvent(
                "Querying {0}, Dataset IDs {1}-{2}",
                "V_Dataset_Files_List_Report", datasetIDs[0], datasetIDs.Last());

            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount: 2);

            if (!success)
            {
                ReportWarning("Error obtaining data from V_Dataset_Files_List_Report for dataset IDs " + GetStartOfString(datasetIdList, 50));
                return false;
            }

            var columnMap = dbTools.GetColumnMapping(columnNames);

            foreach (var resultRow in queryResults)
            {
                var datasetId = dbTools.GetColumnValue(resultRow, columnMap, "dataset_id", -1);
                var fileHash = dbTools.GetColumnValue(resultRow, columnMap, "file_hash");
                var fileSizeBytes = dbTools.GetColumnValue(resultRow, columnMap, "file_size_bytes", (long)0);
                var fileNameOrPath = dbTools.GetColumnValue(resultRow, columnMap, "file_path");

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

            var sqlQuery = GetDatasetInfoQuery(dbTools.DbServerType, datasetNameList, out var columnNames);

            if (quotedDatasetNames.Count == 0)
            {
                OnWarningEvent("quotedDatasetNames is empty in GetDatasetFolderPathInfo; this is unexpected");
            }
            else
            {
                OnDebugEvent(
                    "Querying {0}, Dataset {1}",
                    "V_Dataset_Folder_Paths", quotedDatasetNames[0].Replace("\'", string.Empty));
            }

            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount: 2);

            if (!success)
            {
                ReportWarning("Error obtaining data from V_Dataset_Folder_Paths for datasets " + GetStartOfString(datasetNameList, 50));
                return false;
            }

            var columnMap = dbTools.GetColumnMapping(columnNames);

            foreach (var resultRow in queryResults)
            {
                var datasetName = dbTools.GetColumnValue(resultRow, columnMap, "Dataset");

                if (!datasetNameInfoMap.TryGetValue(datasetName, out var datasetInfo))
                {
                    ReportWarning(string.Format("Dataset {0} not found in datasetNameInfoMap (this is unexpected)", datasetName));
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

                var instrumentDataPurged = dbTools.GetColumnValue(resultRow, columnMap, "Instrument_data_purged", 0);
                datasetInfo.InstrumentDataPurged = IntToBool(instrumentDataPurged);

                var myEmslState = dbTools.GetColumnValue(resultRow, columnMap, "MyEMSL_State", 0);
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

        private string GetDatasetInfoQuery(
            DbServerTypes dbToolsDbServerType,
            string datasetNameList,
            out List<string> columnNames)
        {
            columnNames = new List<string>
            {
                "DFP.Dataset",
                "DFP.Dataset_ID",
                "DFP.Dataset_Folder_Path",
                "DFP.Archive_Folder_Path",
                "DFP.Instrument_data_purged",
            };

            if (dbToolsDbServerType == DbServerTypes.MSSQLServer)
            {
                columnNames.Add("DE.MyEMSL_State");
                columnNames.Add("InstList.class AS Instrument_Class");

                return
                    " SELECT " + string.Join(", ", columnNames) +
                    " FROM V_Dataset_Folder_Paths DFP" +
                    "      INNER JOIN V_Dataset_Export DE ON DFP.dataset_id = DE.id" +
                    "      INNER JOIN V_Instrument_List_Export InstList ON DE.instrument = InstList.name" +
                    " WHERE DFP.dataset IN (" + datasetNameList + ")";
            }

            columnNames.Add("DA.MyEMSL_State");
            columnNames.Add("InstList.class AS Instrument_Class");

            return
                " SELECT " + string.Join(", ", columnNames) +
                " FROM V_Dataset_Folder_Paths DFP" +
                "      INNER JOIN t_dataset DS ON DFP.dataset_id = DS.dataset_ID" +
                "      INNER JOIN V_Instrument_List_Export InstList ON DS.instrument_id = InstList.id " +
                "      LEFT OUTER JOIN t_dataset_archive DA ON DFP.dataset_id = DA.dataset_id" +
                " WHERE DFP.dataset IN (" + datasetNameList + ")";
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
        /// <param name="value">Value to examine</param>
        /// <param name="n">Number of characters to return</param>
        private string GetStartOfString(string value, int n)
        {
            if (value.Length <= n)
                return value;

            return value.Substring(0, n) + " ...";
        }

        private void InitializeDatasetInfoFileColumns()
        {
            DataTableUtils.AddColumnNamesForIdentifier(DatasetInfoColumnNames,
                DatasetInfoColumns.DatasetName,
                "Dataset", "DatasetName", "Dataset Name");

            DataTableUtils.AddColumnNamesForIdentifier(DatasetInfoColumnNames,
                DatasetInfoColumns.TargetName,
                "TargetName", "Target Name", "New Name", "DCC_File_Name");

            DataTableUtils.AddColumnNamesForIdentifier(DatasetInfoColumnNames,
                DatasetInfoColumns.TargetDirectory,
                "TargetDirectory", "Target Directory", "DCC_Folder_Name");
        }

        /// <summary>
        /// Convert value to True or False
        /// </summary>
        /// <param name="value">Value</param>
        /// <returns>True if valueText contains a non-zero integer (positive or negative); otherwise, false</returns>
        private bool IntToBool(int value)
        {
            return value != 0;
        }

        private bool LoadDatasetInfoFile(string datasetInfoFilePath, string outputDirectoryPath, out List<DatasetInfo> datasetList)
        {
            datasetList = new List<DatasetInfo>();

            try
            {
                var outputDirectoryParts = new List<string>();

                if (Path.IsPathRooted(outputDirectoryPath))
                {
                    // Assure that outputDirectoryPath path uses Windows slashes and does not end in a backslash
                    outputDirectoryPath = ChecksumFileUpdater.UpdatePathSeparators(outputDirectoryPath, false).TrimEnd('\\');

                    outputDirectoryParts.AddRange(outputDirectoryPath.Split('\\'));
                }

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

                using var reader = new StreamReader(new FileStream(datasetInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

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
                            ReportWarning("No valid column names were found in the header line of the dataset info file; unable to continue");
                            var defaultHeaderNames = DataTableUtils.GetExpectedHeaderLine(DatasetInfoColumnNames, "   ");
                            OnDebugEvent("Supported headers are:\n  " + defaultHeaderNames);
                            return false;
                        }

                        if (columnMap[DatasetInfoColumns.DatasetName] < 0)
                        {
                            ReportWarning("Dataset info file is missing the Dataset name column on the header line; unable to continue");
                            var defaultHeaderNames = DataTableUtils.GetExpectedHeaderLine(DatasetInfoColumnNames, "   ");
                            OnDebugEvent("Supported headers are:\n  " + defaultHeaderNames);
                            return false;
                        }

                        continue;
                    }

                    var rowData = dataLine.Split('\t').ToList();

                    var datasetName = DataTableUtils.GetColumnValue(rowData, columnMap, DatasetInfoColumns.DatasetName, string.Empty);
                    var targetName = DataTableUtils.GetColumnValue(rowData, columnMap, DatasetInfoColumns.TargetName, string.Empty);
                    var targetDirectory = DataTableUtils.GetColumnValue(rowData, columnMap, DatasetInfoColumns.TargetDirectory, string.Empty);

                    // ReSharper disable CommentTypo

                    // The parameter file defines the output directory path, e.g.
                    // OutputDirectory=F:\Upload\Upload_MoTrPAC\2022August_PASS1A-06\PASS1A-06\T59\PROT_PH\BATCH1_20220826\

                    // Ideally, the dataset info file will have relative paths to append to the output directory path, e.g.
                    // RAW_20220826\01MOTRPAC_PASS1A-06_T59_PH_PN_20220826
                    // RAW_20220826\02MOTRPAC_PASS1A-06_T59_PH_PN_20220826
                    // RAW_20220826\03MOTRPAC_PASS1A-06_T59_PH_PN_20220826

                    // However, if the target directory column in the dataset info file includes additional parent directories, the final output path will be invalid
                    // Examples:
                    // PASS1A-06\T59\PROT_PH\BATCH1_20220826\RAW_20220826\01MOTRPAC_PASS1A-06_T59_PH_PN_20220826
                    // PASS1A-06\T59\PROT_PH\BATCH1_20220826\RAW_20220826\02MOTRPAC_PASS1A-06_T59_PH_PN_20220826
                    // PASS1A-06\T59\PROT_PH\BATCH1_20220826\RAW_20220826\03MOTRPAC_PASS1A-06_T59_PH_PN_20220826

                    // ReSharper restore CommentTypo

                    // The following method checks for this situation, and will shorten targetDirectory to remove the overlapping portion of the path (if an overlap exists)

                    var relativeTargetDirectory = PruneRelativeDirectoryIfOverlap(outputDirectoryParts, targetDirectory);

                    if (string.IsNullOrWhiteSpace(datasetName))
                    {
                        ReportWarning("Skipping line with empty dataset name: " + dataLine);
                    }

                    var datasetInfo = new DatasetInfo(datasetName)
                    {
                        TargetDatasetName = targetName,
                        TargetDirectory = relativeTargetDirectory
                    };

                    datasetList.Add(datasetInfo);
                }

                if (columnMap.Count == 0)
                {
                    ReportWarning("Dataset info file was empty: " + datasetInfoFile.FullName);
                    return false;
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
                var columnNames = new List<string>
                    {
                        "instrument_class",
                        "is_purgeable",
                        "raw_data_type",
                        "comment"
                    };

                var sqlQuery =
                    " SELECT " + string.Join(", ", columnNames) +
                    " FROM V_Instrument_Class_Export";

                OnDebugEvent("Querying V_Instrument_Class_Export");

                var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount: 2);

                if (!success)
                {
                    ReportWarning("Error obtaining data from V_Instrument_Class_Export");
                    return false;
                }

                var columnMap = dbTools.GetColumnMapping(columnNames);

                foreach (var resultRow in queryResults)
                {
                    var instrumentClassName = dbTools.GetColumnValue(resultRow, columnMap, "instrument_class");
                    var isPurgeable = dbTools.GetColumnValue(resultRow, columnMap, "is_purgeable", 0);
                    var rawDataTypeName = dbTools.GetColumnValue(resultRow, columnMap, "raw_data_type");
                    var comment = dbTools.GetColumnValue(resultRow, columnMap, "comment");

                    InstrumentClassData[instrumentClassName] = new InstrumentClassInfo(
                        instrumentClassName,
                        rawDataTypeName,
                        IntToBool(isPurgeable), comment);
                }

                return true;
            }
            catch (Exception ex)
            {
                ReportError("Error in LoadInstrumentClassData", ex);
                return false;
            }
        }

        /// <summary>
        /// Compare directory names in relativeDirectoryPath to the directory names in outputDirectoryParts
        /// If there is overlap, remove the overlapping directories and return an updated relative directory path
        /// </summary>
        /// <param name="outputDirectoryParts">List of directory names, obtained using outputDirectoryPath.Split('\\')</param>
        /// <param name="relativeDirectoryPath">Relative (non-rooted) directory to examine</param>
        /// <returns>Updated relative directory path if overlapped, otherwise the original path</returns>
        private string PruneRelativeDirectoryIfOverlap(IReadOnlyList<string> outputDirectoryParts, string relativeDirectoryPath)
        {
            // Assure that relativeDirectoryPath path uses Windows slashes and does not end in a backslash
            relativeDirectoryPath = ChecksumFileUpdater.UpdatePathSeparators(relativeDirectoryPath, false).TrimEnd('\\');

            // If the target directory overlaps with the output directory path, remove the extra directories
            var targetDirectoryParts = new List<string>();
            targetDirectoryParts.AddRange(relativeDirectoryPath.Split('\\'));

            var directoriesToPrune = new List<string>();
            var candidateRelativePathsToPrune = new List<string>();

            for (var i = outputDirectoryParts.Count; i > 0; i--)
            {
                directoriesToPrune.Clear();

                for (var j = 0; j < targetDirectoryParts.Count && i + j < outputDirectoryParts.Count; j++)
                {
                    if (outputDirectoryParts[i + j].Equals(targetDirectoryParts[j], StringComparison.OrdinalIgnoreCase))
                    {
                        directoriesToPrune.Add(targetDirectoryParts[j]);
                    }
                    else
                    {
                        break;
                    }
                }

                if (directoriesToPrune.Count > 0)
                {
                    candidateRelativePathsToPrune.Add(string.Join("\\", directoriesToPrune));
                }
            }

            switch (candidateRelativePathsToPrune.Count)
            {
                case 0:
                    return relativeDirectoryPath;

                case 1:
                    return relativeDirectoryPath.Substring(candidateRelativePathsToPrune[0].Length + 1);

                default:
                {
                    // Find the longest entry in candidateRelativePathsToPrune
                    // (from https://stackoverflow.com/a/7975983/1179467)

                    var pathToPrune = candidateRelativePathsToPrune.Aggregate(string.Empty, (max, cur) => max.Length > cur.Length ? max : cur);

                    // Second, easier to read option:
                    // pathToPrune = list.OrderByDescending(s => s.Length).First();

                    return relativeDirectoryPath.Substring(pathToPrune.Length + 1);
                }
            }
        }

        private void ReportError(string message, Exception ex)
        {
            OnErrorEvent(message, ex);
            ErrorMessages.Add(message);
        }

        private void ReportProgress(string progressMessage, float percentComplete, double percentCompleteOverall)
        {
            OnStatusEvent(
                "{0:F1}% complete {1}; {2:F1}% complete overall",
                percentComplete, progressMessage.ToLower(), percentCompleteOverall);
        }

        private void ReportWarning(string message)
        {
            OnWarningEvent(message);
            WarningMessages.Add(message);
        }

        /// <summary>
        /// Read the dataset info file and retrieve the instrument data files for the specified datasets
        /// </summary>
        /// <param name="datasetInfoFilePath">Dataset info file path</param>
        /// <param name="outputDirectoryPath">Output directory path</param>
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
                else
                {
                    // Assure that the output directory path uses Windows slashes and does not end in a backslash
                    outputDirectoryPath = ChecksumFileUpdater.UpdatePathSeparators(outputDirectoryPath, false).TrimEnd('\\');
                }

                var datasetInfoLoaded = LoadDatasetInfoFile(datasetInfoFilePath, outputDirectoryPath, out var datasetList);

                if (!datasetInfoLoaded)
                    return false;

                Options.DatasetInfoFilePath = datasetInfoFilePath;

                var outputDirectory = new DirectoryInfo(outputDirectoryPath);

                if (!outputDirectory.Exists)
                {
                    if (Options.PreviewMode)
                    {
                        OnStatusEvent("Preview create directory: " + PathUtils.CompactPathString(outputDirectory.FullName, 200));
                    }
                    else
                    {
                        OnStatusEvent("Creating the output directory: " + PathUtils.CompactPathString(outputDirectory.FullName, 200));
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
        /// <returns>True if success, false if an error</returns>
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
        /// <returns>True if success, false if an error</returns>
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

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(Options.DMSConnectionString, "DMSDatasetRetriever");

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse);
                RegisterEvents(dbTools);

                // Obtain metadata from DMS for datasets in datasetList
                var success = GetDatasetInfoFromDMS(dbTools, datasetList);

                if (!success)
                    return false;

                var copyFileSuccess = CopyDatasetFiles(dbTools, datasetList, outputDirectory);

                if (!copyFileSuccess)
                    return false;

                if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.None)
                    return true;

                return CreateChecksumFiles(datasetList, outputDirectory);
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

            const string headerLine = "** Problems encountered during processing **";
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

        private void FileCopyUtilityOnProgressUpdate(string progressMessage, float percentComplete)
        {
            var percentCompleteOverall = ProcessFilesOrDirectoriesBase.ComputeIncrementalProgress(0, 50, percentComplete);
            ReportProgress(progressMessage, percentComplete, percentCompleteOverall);
        }

        private void FileHashUtilityOnProgressUpdate(string progressMessage, float percentComplete)
        {
            var percentCompleteOverall = ProcessFilesOrDirectoriesBase.ComputeIncrementalProgress(50, 100, percentComplete);
            ReportProgress(progressMessage, percentComplete, percentCompleteOverall);
        }
    }
}
