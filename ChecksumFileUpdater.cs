using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        /// <summary>
        /// Checksum file mode
        /// </summary>
        public DatasetRetrieverOptions.ChecksumFileType ChecksumFileMode { get; }

        /// <summary>
        /// Data file directory
        /// </summary>
        public DirectoryInfo DataFileDirectory { get; }

        /// <summary>
        /// Files in the data file directory
        /// </summary>
        public List<FileInfo> DataFiles { get; }

        /// <summary>
        /// Information on each file, including MD5 and SHA-1 checksums, plus the file size
        /// </summary>
        public Dictionary<string, FileChecksumInfo> DataFileChecksums { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataFileDirectory"></param>
        /// <param name="checksumFileMode"></param>
        public ChecksumFileUpdater(DirectoryInfo dataFileDirectory, DatasetRetrieverOptions.ChecksumFileType checksumFileMode)
        {
            ChecksumFileMode = checksumFileMode;
            DataFileDirectory = dataFileDirectory;
            DataFiles = new List<FileInfo>();
            DataFileChecksums = new Dictionary<string, FileChecksumInfo>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Add a data file to be hashed
        /// </summary>
        /// <param name="dataFile"></param>
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
                    if (DataFileDirectory.Parent == null)
                        throw new DirectoryNotFoundException("Unable to determine the parent directory of " + DataFileDirectory.FullName);

                    return Path.Combine(DataFileDirectory.Parent.FullName, DataFileDirectory.Name + ".cksum");

                case DatasetRetrieverOptions.ChecksumFileType.MoTrPAC:
                    return Path.Combine(DataFileDirectory.FullName, DataFileDirectory.Name + "_MANIFEST.txt");

                default:
                    throw new Exception("Unrecognized enum value: " + ChecksumFileMode);
            }
        }

        /// <summary>
        /// Load an existing checksum file (if it exists)
        /// </summary>
        public void LoadExistingChecksumFile()
        {
            try
            {
                var checksumFilePath = GetChecksumFilePath();
                if (string.IsNullOrWhiteSpace(checksumFilePath))
                {
                    OnWarningEvent(string.Format(
                        "Checksum file name could not be determined for {0}, ChecksumFileMode {1}",
                        DataFileDirectory.FullName, ChecksumFileMode));
                    return;
                }

                var checksumFile = new FileInfo(checksumFilePath);
                if (!checksumFile.Exists)
                {
                    return;
                }

                OnDebugEvent("Loading existing checksum file: " + checksumFilePath);

                var columnMap = new Dictionary<ChecksumFileColumns, int>();
                var standardColumnNames = new Dictionary<ChecksumFileColumns, SortedSet<string>>();

                if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.CPTAC)
                {
                    columnMap.Add(ChecksumFileColumns.SHA1, 0);
                    columnMap.Add(ChecksumFileColumns.Filename, 1);
                }
                else
                {
                    DataTableUtils.AddColumnNamesForIdentifier(standardColumnNames, ChecksumFileColumns.Filename, "raw_file");
                    DataTableUtils.AddColumnNamesForIdentifier(standardColumnNames, ChecksumFileColumns.Fraction, "fraction");
                    DataTableUtils.AddColumnNamesForIdentifier(standardColumnNames, ChecksumFileColumns.TechnicalReplicate, "technical_replicate");
                    DataTableUtils.AddColumnNamesForIdentifier(standardColumnNames, ChecksumFileColumns.Comment, "tech_rep_comment");
                    DataTableUtils.AddColumnNamesForIdentifier(standardColumnNames, ChecksumFileColumns.MD5, "md5");
                    DataTableUtils.AddColumnNamesForIdentifier(standardColumnNames, ChecksumFileColumns.SHA1, "sha1");
                }

                using (var reader = new StreamReader(new FileStream(checksumFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var lineParts = dataLine.Split('\t').ToList();

                        linesRead++;

                        if (linesRead == 1 && ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC)
                        {
                            // Parse the header line
                            UpdateColumnMapping(standardColumnNames, columnMap, lineParts);
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
                    cleanFileName, DataFileDirectory.FullName));
            }

            var fileChecksumInfo = new FileChecksumInfo(cleanFileName) {
                SHA1 = sha1
            };

            DataFileChecksums.Add(cleanFileName, fileChecksumInfo);
        }

        private void ParseChecksumFileLineMoTrPAC(IReadOnlyDictionary<ChecksumFileColumns, int> columnMap, IReadOnlyList<string> lineParts)
        {
            var fileName = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.Filename);
            var fraction = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.Fraction);
            if (!int.TryParse(fraction, out var fractionNumber))
            {
                fractionNumber = 0;
                OnDebugEvent(string.Format(
                    "The fraction number for file {0} in directory {1} is not numeric; will store 0",
                    fileName, DataFileDirectory.Name));
            }

            var technicalReplicateFlag = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.TechnicalReplicate);
            var comment = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.Comment);
            var md5 = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.MD5);
            var sha1 = DataTableUtils.GetColumnValue(lineParts, columnMap, ChecksumFileColumns.SHA1);

            var isTechnicalReplicate = technicalReplicateFlag.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
                                       technicalReplicateFlag.Equals("true", StringComparison.OrdinalIgnoreCase);

            if (DataFileChecksums.ContainsKey(fileName))
            {
                OnDebugEvent(string.Format(
                    "Checksum file has multiple entries; skipping duplicate file {0} in directory {1}",
                    fileName, DataFileDirectory.FullName));
                return;
            }

            var fileChecksumInfo = new FileChecksumInfo(fileName)
            {
                Fraction = fractionNumber,
                IsTechnicalReplicate = isTechnicalReplicate,
                Comment = comment,
                MD5 = md5,
                SHA1 = sha1
            };

            DataFileChecksums.Add(fileName, fileChecksumInfo);
        }

        private void UpdateColumnMapping(
            Dictionary<ChecksumFileColumns, SortedSet<string>> standardColumnNames,
            IDictionary<ChecksumFileColumns, int> columnMap,
            IReadOnlyList<string> lineParts)
        {
            for (var columnIndex = 0; columnIndex < lineParts.Count; columnIndex++)
            {
                foreach (var standardColumn in standardColumnNames)
                {
                    if (!standardColumn.Value.Contains(lineParts[columnIndex]))
                        continue;

                    columnMap.Add(standardColumn.Key, columnIndex);
                    break;
                }
            }

        }
    }
}
