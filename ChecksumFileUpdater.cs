using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;
using PRISMDatabaseUtils;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// Utility for reading/writing checksum files
    /// </summary>
    internal class ChecksumFileUpdater : EventNotifier
    {
        private enum ChecksumFileColumns
        {
            /// <summary>
            /// File name
            /// </summary>
            Filename = 0,

            /// <summary>
            /// MD5 sum
            /// </summary>
            MD5 = 1,

            /// <summary>
            /// SHA-1 sum
            /// </summary>
            SHA1 = 2,

            /// <summary>
            /// Fraction number (integer)
            /// </summary>
            /// <remarks>Only present in MoTrPAC manifest files</remarks>
            Fraction = 3,

            /// <summary>
            /// Technical replicate flag (yes or no)
            /// </summary>
            /// <remarks>Only present in MoTrPAC manifest files (column technical_replicate)</remarks>
            TechnicalReplicate = 4,

            /// <summary>
            /// File comment
            /// </summary>
            /// <remarks>Only present in MoTrPAC manifest files (column tech_rep_comment)</remarks>
            Comment = 5
        }

        #region "Properties"

        /// <summary>
        /// Base output directory (used when checksumFileMode is ChecksumFileType.MoTrPAC)
        /// </summary>
        public string BaseOutputDirectoryPath { get; }

        /// <summary>
        /// Checksum file mode
        /// </summary>
        public DatasetRetrieverOptions.ChecksumFileType ChecksumFileMode { get; }

        /// <summary>
        /// Date to use when generating the timestamp for the checksum file (used when checksumFileMode is ChecksumFileType.MoTrPAC)
        /// </summary>
        public DateTime ChecksumFileNameDate { get; }

        /// <summary>
        /// Checksum file path
        /// </summary>
        /// <remarks>Auto determined using ChecksumFileMode and ChecksumFileDirectory</remarks>
        public string ChecksumFilePath { get; }

        /// <summary>
        /// Checksum file directory
        /// </summary>
        public DirectoryInfo ChecksumFileDirectory { get; }

        /// <summary>
        /// Files in the data file directory
        /// </summary>
        public List<FileInfo> DataFiles { get; }

        /// <summary>
        /// Information on each file, including MD5 and SHA-1 checksums, plus the file size
        /// Keys are the file name (or relative file path), Values are the checksum details
        /// </summary>
        public Dictionary<string, FileChecksumInfo> DataFileChecksums { get; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="checksumFileDirectory">Directory where the checksum file resides</param>
        /// <param name="checksumFileMode">Checksum mode</param>
        /// <param name="baseOutputDirectoryPath">
        /// Base output directory (used when checksumFileMode is ChecksumFileType.MoTrPAC)
        /// </param>
        /// <param name="checksumFileNameDate">
        /// Date to use when generating the timestamp for the checksum file (used when checksumFileMode is ChecksumFileType.MoTrPAC)
        /// </param>
        public ChecksumFileUpdater(
            DirectoryInfo checksumFileDirectory,
            DatasetRetrieverOptions.ChecksumFileType checksumFileMode,
            string baseOutputDirectoryPath,
            DateTime checksumFileNameDate)
        {
            ChecksumFileMode = checksumFileMode;
            ChecksumFileDirectory = checksumFileDirectory;
            DataFiles = new List<FileInfo>();
            DataFileChecksums = new Dictionary<string, FileChecksumInfo>(StringComparer.OrdinalIgnoreCase);

            BaseOutputDirectoryPath = baseOutputDirectoryPath;
            ChecksumFileNameDate = checksumFileNameDate;
            ChecksumFilePath = GetChecksumFilePath();
        }

        /// <summary>
        /// Add a data file to be hashed
        /// </summary>
        /// <param name="dataFile">Data file info</param>
        public void AddDataFile(FileInfo dataFile)
        {
            DataFiles.Add(dataFile);
        }

        private string GetChecksumFilePath()
        {
            switch (ChecksumFileMode)
            {
                case DatasetRetrieverOptions.ChecksumFileType.None:
                    return string.Empty;

                case DatasetRetrieverOptions.ChecksumFileType.CPTAC:
                    if (ChecksumFileDirectory.Parent == null)
                        throw new DirectoryNotFoundException("Unable to determine the parent directory of " + ChecksumFileDirectory.FullName);

                    return Path.Combine(ChecksumFileDirectory.Parent.FullName, ChecksumFileDirectory.Name + ".cksum");

                case DatasetRetrieverOptions.ChecksumFileType.MoTrPAC:
                    // Behavior in 2020 was to create a separate manifest file in each data file directory
                    // return Path.Combine(DataFileDirectory.FullName, DataFileDirectory.Name + "_MANIFEST.txt");

                    // New behavior in 2021 is to create a single manifest file in the base output directory

                    string baseOutputDirectoryPath;
                    if (string.IsNullOrWhiteSpace(BaseOutputDirectoryPath))
                    {
                        if (ChecksumFileDirectory.Parent == null)
                            throw new DirectoryNotFoundException("Unable to determine the parent directory of " + ChecksumFileDirectory.FullName);

                        baseOutputDirectoryPath = ChecksumFileDirectory.Parent.FullName;
                    }
                    else
                    {
                        baseOutputDirectoryPath = BaseOutputDirectoryPath;
                    }

                    var checksumFileNameDate = ChecksumFileNameDate == DateTime.MinValue ? DateTime.Now : ChecksumFileNameDate;

                    var checksumFileName = string.Format("file_manifest_{0:yyyyMMdd}.csv", checksumFileNameDate);

                    return Path.Combine(baseOutputDirectoryPath, checksumFileName);

                default:
                    throw new Exception("Unrecognized enum value: " + ChecksumFileMode);
            }
        }

        private string GetExpectedHeaderLine(IReadOnlyDictionary<ChecksumFileColumns, SortedSet<string>> columnNamesByIdentifier)
        {
            var columnIdentifierList = new List<ChecksumFileColumns>();

            if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.CPTAC)
            {
                columnIdentifierList.Add(ChecksumFileColumns.SHA1);
                columnIdentifierList.Add(ChecksumFileColumns.Filename);
            }

            if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC)
            {
                columnIdentifierList.Add(ChecksumFileColumns.Filename);
                columnIdentifierList.Add(ChecksumFileColumns.Fraction);
                columnIdentifierList.Add(ChecksumFileColumns.TechnicalReplicate);
                columnIdentifierList.Add(ChecksumFileColumns.Comment);
                columnIdentifierList.Add(ChecksumFileColumns.MD5);
                columnIdentifierList.Add(ChecksumFileColumns.SHA1);
            }

            return DataTableUtils.GetExpectedHeaderLine(columnNamesByIdentifier, columnIdentifierList, "   ");
        }

        /// <summary>
        /// Load an existing checksum file (if it exists)
        /// </summary>
        public void LoadExistingChecksumFile()
        {
            try
            {
                if (string.IsNullOrWhiteSpace(ChecksumFilePath))
                {
                    if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.None)
                        return;

                    OnWarningEvent(string.Format(
                        "Checksum file name could not be determined for {0} in LoadExistingChecksumFile; ChecksumFileMode is {1}",
                        ChecksumFileDirectory.FullName, ChecksumFileMode));
                    return;
                }

                var defaultChecksumFile = new FileInfo(ChecksumFilePath);
                if (defaultChecksumFile.Directory == null)
                {
                    OnWarningEvent(string.Format(
                        "Unable to determine the parent directory of the default checksum file: {0}",
                        ChecksumFilePath));
                    return;
                }

                // Keys in this list are directory paths
                // Values are checksum filename, optionally with a wildcard
                var directoriesToCheck = new List<DirectoryInfo>
                {
                    defaultChecksumFile.Directory,
                    new DirectoryInfo(BaseOutputDirectoryPath)
                };

                var checksumFileNames = new List<string>();

                // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
                switch (ChecksumFileMode)
                {
                    case DatasetRetrieverOptions.ChecksumFileType.CPTAC:
                        checksumFileNames.Add("*.cksum");
                        break;

                    case DatasetRetrieverOptions.ChecksumFileType.MoTrPAC:
                        checksumFileNames.Add("*_manifest_*.csv");
                        checksumFileNames.Add("*_MANIFEST.txt");
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                FileInfo checksumFile = null;

                foreach (var directory in directoriesToCheck.Where(directory => directory.Exists))
                {
                    var candidateChecksumFile = new FileInfo(Path.Combine(directory.FullName, defaultChecksumFile.Name));
                    if (candidateChecksumFile.Exists && candidateChecksumFile.Length > 0)
                    {
                        checksumFile = defaultChecksumFile;
                        break;
                    }

                    foreach (var fileSpec in checksumFileNames)
                    {
                        var checksumFiles = directory.GetFiles(fileSpec).ToList();

                        foreach (var item in checksumFiles.Where(item => item.Length > 0))
                        {
                            checksumFile = item;
                            break;
                        }

                        if (checksumFile != null)
                            break;
                    }

                    if (checksumFile != null)
                        break;
                }

                if (checksumFile == null)
                {
                    OnWarningEvent(string.Format(
                        "Checksum file name could not be determined for {0} in LoadExistingChecksumFile; ChecksumFileMode is {1}",
                        ChecksumFileDirectory.FullName, ChecksumFileMode));

                    return;
                }

                OnDebugEvent("Loading existing checksum file: " + PathUtils.CompactPathString(checksumFile.FullName, 100));

                var columnMap = new Dictionary<ChecksumFileColumns, int>();
                var columnNamesByIdentifier = new Dictionary<ChecksumFileColumns, SortedSet<string>>();
                char columnDelimiter;

                if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.CPTAC)
                {
                    columnMap.Add(ChecksumFileColumns.SHA1, 0);
                    columnMap.Add(ChecksumFileColumns.Filename, 1);
                    columnDelimiter = '\t';
                }
                else
                {
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.Filename, "file_name", "raw_file");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.Fraction, "fraction");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.TechnicalReplicate, "technical_replicate");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.Comment, "tech_rep_comment");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.MD5, "md5");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.SHA1, "sha1");

                    columnDelimiter = checksumFile.Extension.Equals(".csv", StringComparison.OrdinalIgnoreCase) ? ',' : '\t';
                }

                using (var reader = new StreamReader(new FileStream(checksumFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        linesRead++;

                        var lineParts = dataLine.Split(columnDelimiter).ToList();

                        if (linesRead == 1 && ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC)
                        {
                            // Parse the header line
                            var headerLine = string.Join("\t", lineParts);

                            var validHeaders = DataTableUtils.GetColumnMappingFromHeaderLine(columnMap, headerLine, columnNamesByIdentifier);
                            if (!validHeaders)
                            {
                                OnWarningEvent("The checksum file header line does not contain the expected columns:\n  " + dataLine);
                                var defaultHeaderNames = GetExpectedHeaderLine(columnNamesByIdentifier);
                                OnDebugEvent("Supported headers are: " + defaultHeaderNames);
                                break;
                            }

                            continue;
                        }

                        switch (ChecksumFileMode)
                        {
                            case DatasetRetrieverOptions.ChecksumFileType.MoTrPAC:
                                ParseChecksumFileLineMoTrPAC(columnMap, lineParts);
                                break;

                            case DatasetRetrieverOptions.ChecksumFileType.CPTAC:
                                ParseChecksumFileLineCPTAC(columnMap, lineParts);
                                break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in LoadExistingChecksumFile", ex);
            }
        }

        private void ParseChecksumFileLineCPTAC(IReadOnlyDictionary<ChecksumFileColumns, int> columnMap, IReadOnlyList<string> lineParts)
        {
            var sha1 = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.SHA1);
            var fileName = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.Filename);

            string cleanFileName;
            if (fileName.StartsWith("*"))
                cleanFileName = fileName.Substring(1);
            else
                cleanFileName = fileName;

            if (DataFileChecksums.ContainsKey(cleanFileName))
            {
                OnDebugEvent(string.Format(
                    "Checksum file has multiple entries; skipping duplicate file {0} in directory {1}",
                    cleanFileName, ChecksumFileDirectory.FullName));
            }

            var fileChecksumInfo = new FileChecksumInfo(cleanFileName)
            {
                SHA1 = sha1
            };

            DataFileChecksums.Add(cleanFileName, fileChecksumInfo);
        }

        private void ParseChecksumFileLineMoTrPAC(IReadOnlyDictionary<ChecksumFileColumns, int> columnMap, IReadOnlyList<string> lineParts)
        {
            var relativeFilePath = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.Filename);
            var md5 = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.MD5);
            var sha1 = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.SHA1);

            if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC &&
                BaseOutputDirectoryPath.Length > 0 &&
                relativeFilePath.IndexOf("/", StringComparison.Ordinal) < 0)
            {
                // relativeFilePath only has a filename and not a Linux-style relative path
                var nameToFind = Path.GetFileName(relativeFilePath);
                var baseOutputDirectoryName = Path.GetFileName(BaseOutputDirectoryPath);

                // Look for a match to the filename in DataFiles
                foreach (var dataFile in DataFiles.Where(dataFile => dataFile.Name.Equals(nameToFind)))
                {
                    if (dataFile.FullName.StartsWith(BaseOutputDirectoryPath, StringComparison.OrdinalIgnoreCase))
                    {
                        relativeFilePath = Path.Combine(baseOutputDirectoryName, dataFile.FullName.Substring(BaseOutputDirectoryPath.Length).Trim('\\'));
                    }

                    break;
                }
            }

            relativeFilePath = UpdatePathSeparators(relativeFilePath);

            if (DataFileChecksums.ContainsKey(relativeFilePath))
            {
                OnDebugEvent(string.Format(
                    "Checksum file has multiple entries; skipping duplicate file {0} in directory {1}",
                    relativeFilePath, ChecksumFileDirectory.FullName));
                return;
            }

            var fileChecksumInfo = new FileChecksumInfo(relativeFilePath)
            {
                Fraction = fractionNumber,
                IsTechnicalReplicate = isTechnicalReplicate,
                Comment = comment,
                MD5 = md5,
                SHA1 = sha1
            };

            if (technicalReplicateFlag.Equals("no") && comment.Equals("no"))

        /// <summary>
        /// Assure that directory separators in filePath are \ or /, depending on useLinuxSlashes
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="useLinuxSlashes">When true, use / for directory separators; otherwise use \</param>
        public static string UpdatePathSeparators(string filePath, bool useLinuxSlashes = true)
        {
            if (useLinuxSlashes)
            {
                return filePath.Replace('\\', '/');
            }

            return filePath.Replace('/', '\\');
        }

        /// <summary>
        /// Create or update the checksum file
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool WriteChecksumFile()
        {
            try
            {
                var checksumFilePath = GetChecksumFilePath();
                if (string.IsNullOrWhiteSpace(checksumFilePath))
                {
                    if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.None)
                    {
                        OnWarningEvent(string.Format(
                            "WriteChecksumFile called when the ChecksumFileMode is {0}; nothing to do",
                            ChecksumFileMode));
                        return true;
                    }

                    OnWarningEvent(string.Format(
                        "Checksum file name could not be determined for {0} in WriteChecksumFile; ChecksumFileMode is {1}",
                        ChecksumFileDirectory.FullName, ChecksumFileMode));
                    return false;
                }

                var checksumFile = new FileInfo(checksumFilePath);
                if (checksumFile.Exists)
                {
                    OnDebugEvent("Updating existing checksum file: " + PathUtils.CompactPathString(checksumFilePath, 100));
                }
                else
                {
                    OnDebugEvent("Creating new checksum file: " + PathUtils.CompactPathString(checksumFilePath, 100));
                }

                using (var writer = new StreamWriter(new FileStream(checksumFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    WriteHeaderLine(writer);

                    foreach (var dataFile in DataFileChecksums)
                    {
                        WriteChecksumLine(writer, dataFile);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in WriteChecksumFile", ex);
                return false;
            }
        }

        private void WriteHeaderLine(TextWriter writer)
        {
            switch (ChecksumFileMode)
            {
                case DatasetRetrieverOptions.ChecksumFileType.MoTrPAC:
                    var columnNames = new List<string>
                    {
                        "fraction",
                        "technical_replicate",
                        "tech_rep_comment",
                        "file_name",
                        "md5",
                        "sha1"
                    };

                    writer.WriteLine(string.Join(",", columnNames));
                    return;

                case DatasetRetrieverOptions.ChecksumFileType.CPTAC:
                    // CPTAC checksum files do not have a header line
                    return;
            }
        }

        private void WriteChecksumLine(TextWriter writer, KeyValuePair<string, FileChecksumInfo> dataFile)
        {
            var dataFileInfo = dataFile.Value;

            // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
            switch (ChecksumFileMode)
            {
                case DatasetRetrieverOptions.ChecksumFileType.MoTrPAC:
                    string relativeFilePath;
                    if (dataFileInfo.FilePath.Contains('/'))
                    {
                        // We already have a Linux-style relative file path; use as-is
                        relativeFilePath = dataFileInfo.FilePath;
                    }
                    else if (string.IsNullOrWhiteSpace(BaseOutputDirectoryPath) || !dataFileInfo.FilePath.StartsWith(BaseOutputDirectoryPath))
                    {
                        relativeFilePath = dataFileInfo.FileName;
                    }
                    else
                    {
                        relativeFilePath = dataFileInfo.FilePath.Substring(BaseOutputDirectoryPath.Length);
                    }

                    var dataValues = new List<string>
                    {
                        dataFileInfo.Fraction.ToString(),
                        dataFileInfo.IsTechnicalReplicate ? "yes" : "no",
                        dataFileInfo.Comment,
                        relativeFilePath,
                        dataFileInfo.MD5,
                        dataFileInfo.SHA1
                    };

                    writer.WriteLine(string.Join(",", dataValues));
                    return;

                case DatasetRetrieverOptions.ChecksumFileType.CPTAC:
                    writer.WriteLine("{0}\t*{1}", dataFileInfo.SHA1, dataFileInfo.FileName);
                    return;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// Show the checksum file path
        /// </summary>
        public override string ToString()
        {
            return PathUtils.CompactPathString(ChecksumFilePath);
        }
    }
}
