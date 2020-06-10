using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// Utility for computing MD5 and SHA-1 checksums
    /// </summary>
    internal class FileHashUtility : EventNotifier
    {
        /// <summary>
        /// Retrieval options
        /// </summary>
        private DatasetRetrieverOptions Options { get; }

        private string ComputeChecksumMD5(FileSystemInfo dataFile)
        {
            var md5 = HashUtilities.ComputeFileHashMD5(dataFile.FullName);
            return md5;
        }

        private string ComputeChecksumSHA1(FileSystemInfo dataFile)
        {
            var sha1 = HashUtilities.ComputeFileHashSha1(dataFile.FullName);
            return sha1;
        }

        private long ComputeTotalBytesToHash(ChecksumFileUpdater checksumUpdater)
        {
            long totalBytesToHash = 0;

            foreach (var dataFile in checksumUpdater.DataFiles)
            {
                if (!dataFile.Exists)
                    continue;

                var fileChecksumInfo = GetFileChecksumInfo(checksumUpdater, dataFile);

                if (string.IsNullOrWhiteSpace(fileChecksumInfo.SHA1))
                {
                    totalBytesToHash += dataFile.Length;
                }

                if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC &&
                    string.IsNullOrWhiteSpace(fileChecksumInfo.MD5))
                {
                    totalBytesToHash += dataFile.Length;
                }
            }

            return totalBytesToHash;
        }

        /// <summary>
        /// Create (or update) the checksum file for each output directory
        /// </summary>
        /// <param name="datasetList"></param>
        /// <returns></returns>
        public bool CreateChecksumFiles(IEnumerable<DatasetInfo> datasetList)
        {

            try
            {
                // Keys in this dictionary are the full path to the target directory
                // Values are ChecksumFileUpdater instances tracking the files to hash
                var checksumData = new Dictionary<string, ChecksumFileUpdater>();

                foreach (var dataset in datasetList)
                {
                    if (dataset.TargetDirectoryFiles.Count == 0)
                    {
                        if (dataset.DatasetID > 0)
                        {
                            OnWarningEvent(string.Format(
                                "Dataset {0} does not have any files in the target directory: {1}",
                                dataset.DatasetName, dataset.TargetDirectory));
                        }

                        continue;
                    }

                    var targetDirectory = dataset.TargetDirectoryFiles.First().Directory;

                    var checksumUpdater = GetChecksumUpdater(checksumData, targetDirectory);

                    foreach (var datasetFile in dataset.TargetDirectoryFiles)
                    {
                        checksumUpdater.AddDataFile(datasetFile);
                    }
                }

                var successCount = 0;
                foreach (var item in checksumData)
                {
                    var updateSuccess = CreateOrUpdateChecksumFile(item.Value);
                    if (updateSuccess)
                        successCount++;
                }

                return (successCount == checksumData.Count);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateChecksumFiles", ex);
                return false;
            }
        }

        private bool CreateOrUpdateChecksumFile(ChecksumFileUpdater checksumUpdater)
        {
            try
            {
                checksumUpdater.LoadExistingChecksumFile();

                var totalBytesToHash = ComputeTotalBytesToHash(checksumUpdater);

                long totalBytesHashed = 0;
                var lastProgressTime = DateTime.UtcNow;

                foreach (var dataFile in checksumUpdater.DataFiles)
                {
                    var fileChecksumInfo = GetFileChecksumInfo(checksumUpdater, dataFile);

                    if (string.IsNullOrWhiteSpace(fileChecksumInfo.SHA1))
                    {
                        if (Options.PreviewMode)
                        {
                            OnDebugEvent("Compute SHA-1 sum of " + dataFile.Name);
                        }
                        else
                        {
                            if (!dataFile.Exists)
                            {
                                OnWarningEvent("File not found; cannot compute the SHA-1 hash of " + dataFile.FullName);
                                continue;
                            }

                            fileChecksumInfo.SHA1 = ComputeChecksumSHA1(dataFile);
                            totalBytesHashed += dataFile.Length;
                        }

                    }

                    if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC &&
                        string.IsNullOrWhiteSpace(fileChecksumInfo.MD5))
                    {
                        if (Options.PreviewMode)
                        {
                            OnDebugEvent("Compute MD5 sum of " + dataFile.Name);
                        }
                        else
                        {
                            if (!dataFile.Exists)
                            {
                                OnWarningEvent("File not found; cannot compute the MD5 hash of " + dataFile.FullName);
                                continue;
                            }

                            fileChecksumInfo.MD5 = ComputeChecksumMD5(dataFile);
                            totalBytesHashed += dataFile.Length;
                        }

                    }

                    if (totalBytesToHash <= 0 || Options.PreviewMode || DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds < 3)
                        continue;

                    var percentComplete = totalBytesHashed / (float)totalBytesToHash * 100;
                    OnProgressUpdate("Computing checksums", percentComplete);
                    lastProgressTime = DateTime.UtcNow;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateOrUpdateChecksumFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public FileHashUtility(DatasetRetrieverOptions options)
        {
            Options = options;
        }

        private ChecksumFileUpdater GetChecksumUpdater(
            IDictionary<string, ChecksumFileUpdater> checksumData,
            DirectoryInfo targetDirectory)
        {
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (checksumData.TryGetValue(targetDirectory.FullName, out var checksumUpdater))
            {
                return checksumUpdater;
            }

            var newUpdater = new ChecksumFileUpdater(targetDirectory, Options.ChecksumFileMode);
            checksumData.Add(targetDirectory.FullName, newUpdater);

            return newUpdater;
        }

        private FileChecksumInfo GetFileChecksumInfo(ChecksumFileUpdater checksumUpdater, FileSystemInfo dataFile)
        {
            if (checksumUpdater.DataFileChecksums.TryGetValue(dataFile.Name, out var fileChecksumInfo))
            {
                return fileChecksumInfo;
            }

            var newChecksumInfo = new FileChecksumInfo(dataFile.Name);
            checksumUpdater.DataFileChecksums.Add(dataFile.Name, newChecksumInfo);

            return newChecksumInfo;
        }
    }
}
