﻿using System;
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
        /// Checksum file mode
        /// </summary>
        public DatasetRetrieverOptions.ChecksumFileType ChecksumFileMode { get; }

        /// <summary>
        /// Checksum file path
        /// </summary>
        /// <remarks>auto determined using ChecksumFileMode and DataFileDirectory</remarks>
        public string ChecksumFilePath { get; }

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
        /// Keys are the file name, Values are the checksum details
        /// </summary>
        public Dictionary<string, FileChecksumInfo> DataFileChecksums { get; }

        #endregion

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

            ChecksumFilePath = GetChecksumFilePath();
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
                        DataFileDirectory.FullName, ChecksumFileMode));
                    return;
                }

                var checksumFile = new FileInfo(ChecksumFilePath);
                if (!checksumFile.Exists)
                {
                    return;
                }

                OnDebugEvent("Loading existing checksum file: " + PathUtils.CompactPathString(ChecksumFilePath, 100));

                var columnMap = new Dictionary<ChecksumFileColumns, int>();
                var columnNamesByIdentifier = new Dictionary<ChecksumFileColumns, SortedSet<string>>();

                if (ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.CPTAC)
                {
                    columnMap.Add(ChecksumFileColumns.SHA1, 0);
                    columnMap.Add(ChecksumFileColumns.Filename, 1);
                }
                else
                {
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.Filename, "raw_file");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.Fraction, "fraction");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.TechnicalReplicate, "technical_replicate");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.Comment, "tech_rep_comment");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.MD5, "md5");
                    DataTableUtils.AddColumnNamesForIdentifier(columnNamesByIdentifier, ChecksumFileColumns.SHA1, "sha1");
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

                        if (linesRead == 1 && ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC)
                        {
                            // Parse the header line
                            var validHeaders = DataTableUtils.GetColumnMappingFromHeaderLine(columnMap, dataLine, columnNamesByIdentifier);
                            if (!validHeaders)
                            {
                                OnWarningEvent("The checksum file header line does not contain the expected columns:\n  " + dataLine);
                                var defaultHeaderNames = GetExpectedHeaderLine(columnNamesByIdentifier);
                                OnDebugEvent("Supported headers are: " + defaultHeaderNames);
                            }

                            continue;
                        }

                        var lineParts = dataLine.Split('\t').ToList();

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

            if (technicalReplicateFlag.Equals("no") && comment.Equals("no"))
            {
                // Fix typo in _Manifest.txt files
                fileChecksumInfo.Comment = string.Empty;
            }

            DataFileChecksums.Add(fileName, fileChecksumInfo);
        }

        /// <summary>
        /// Create or update the checksum file
        /// </summary>
        /// <returns></returns>
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
                        DataFileDirectory.FullName, ChecksumFileMode));
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
                        "raw_file",
                        "fraction",
                        "technical_replicate",
                        "tech_rep_comment",
                        "md5",
                        "sha1"
                    };

                    writer.WriteLine(string.Join("\t", columnNames));
                    return;

                case DatasetRetrieverOptions.ChecksumFileType.CPTAC:
                    // CPTAC checksum files do not have a header line
                    return;
            }
        }

        private void WriteChecksumLine(TextWriter writer, KeyValuePair<string, FileChecksumInfo> dataFile)
        {
            var dataFileInfo = dataFile.Value;

            switch (ChecksumFileMode)
            {
                case DatasetRetrieverOptions.ChecksumFileType.MoTrPAC:
                    var dataValues = new List<string>
                    {
                        dataFileInfo.FileName,
                        dataFileInfo.Fraction.ToString(),
                        dataFileInfo.IsTechnicalReplicate ? "yes" : "no",
                        dataFileInfo.Comment,
                        dataFileInfo.MD5,
                        dataFileInfo.SHA1
                    };

                    writer.WriteLine(string.Join("\t", dataValues));
                    return;

                case DatasetRetrieverOptions.ChecksumFileType.CPTAC:
                    writer.WriteLine("{0}\t*{1}", dataFileInfo.SHA1, dataFileInfo.FileName);
                    return;
            }
        }
    }
}
