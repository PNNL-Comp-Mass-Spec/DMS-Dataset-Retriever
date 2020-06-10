using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM;
using PRISM.FileProcessor;

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

        /// <summary>
        /// Compute checksums for files in checksumFileUpdater.DataFiles
        /// </summary>
        /// <param name="checksumFileUpdater"></param>
        /// <param name="progressAtStart"></param>
        /// <param name="progressAtEnd"></param>
        /// <returns></returns>
        public bool ComputeFileChecksums(ChecksumFileUpdater checksumFileUpdater, float progressAtStart, float progressAtEnd)
        {

            try
            {
                var totalBytesToHash = ComputeTotalBytesToHash(checksumFileUpdater, out var datasetCountToProcess);

                long totalBytesHashed = 0;
                var lastProgressTime = DateTime.UtcNow;

                if (datasetCountToProcess == 0)
                {
                    Console.WriteLine();
                    OnStatusEvent(string.Format(
                        "Checksum values are already up to date for all {0} datasets in {1}",
                        checksumFileUpdater.DataFiles.Count,
                        Path.GetFileName(checksumFileUpdater.ChecksumFilePath)));

                    return true;
                }

                Console.WriteLine();
                var action = Options.PreviewMode ? "Preview compute" : "Computing";

                OnStatusEvent(string.Format(
                    "{0} checksum values for {1} datasets in {2}; {3} to process ",
                    action,
                    datasetCountToProcess,
                    Path.GetFileName(checksumFileUpdater.ChecksumFilePath),
                    FileTools.BytesToHumanReadable(totalBytesToHash)));

                foreach (var dataFile in checksumFileUpdater.DataFiles)
                {
                    var fileChecksumInfo = GetFileChecksumInfo(checksumFileUpdater, dataFile);

                    var computeSHA1 = string.IsNullOrWhiteSpace(fileChecksumInfo.SHA1);

                    var computeMD5 = string.IsNullOrWhiteSpace(fileChecksumInfo.MD5) &&
                                     Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC;

                    if (Options.PreviewMode)
                    {
                        if (computeMD5 && computeSHA1)
                        {
                            OnDebugEvent("Compute SHA-1 and MD5 hashes: " + dataFile.Name);
                        }
                        else if (computeMD5)
                        {
                            OnDebugEvent("Compute MD5 hash: " + dataFile.Name);
                        }
                        else if (computeSHA1)
                        {
                            OnDebugEvent("Compute SHA-1 hash: " + dataFile.Name);
                        }
                        continue;
                    }

                    if (computeSHA1)
                    {
                        if (!dataFile.Exists)
                        {
                            OnWarningEvent("File not found; cannot compute the SHA-1 hash for " + dataFile.FullName);
                            continue;
                        }

                        OnDebugEvent("Computing SHA-1 hash: " + dataFile.Name);
                        fileChecksumInfo.SHA1 = ComputeChecksumSHA1(dataFile);
                        totalBytesHashed += dataFile.Length;
                    }

                    if (computeMD5)
                    {
                        if (!dataFile.Exists)
                        {
                            OnWarningEvent("File not found; cannot compute the MD5 hash for " + dataFile.FullName);
                            continue;
                        }

                        OnDebugEvent("Computing MD5 hash:   " + dataFile.Name);
                        fileChecksumInfo.MD5 = ComputeChecksumMD5(dataFile);
                        totalBytesHashed += dataFile.Length;
                    }

                    if (totalBytesToHash <= 0 || Options.PreviewMode || DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds < 15)
                        continue;

                    var checksumPercentComplete = totalBytesHashed / (float)totalBytesToHash * 100;

                    var percentComplete = ProcessFilesOrDirectoriesBase.ComputeIncrementalProgress(progressAtStart, progressAtEnd, checksumPercentComplete);

                    OnProgressUpdate("Computing checksums", percentComplete);
                    lastProgressTime = DateTime.UtcNow;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ComputeFileChecksums", ex);
                return false;
            }
        }

        private long ComputeTotalBytesToHash(ChecksumFileUpdater checksumFileUpdater, out int datasetCountToProcess)
        {
            long totalBytesToHash = 0;
            datasetCountToProcess = 0;

            foreach (var dataFile in checksumFileUpdater.DataFiles)
            {
                if (!dataFile.Exists)
                    continue;

                var fileChecksumInfo = GetFileChecksumInfo(checksumFileUpdater, dataFile);
                var updateRequired = false;

                if (string.IsNullOrWhiteSpace(fileChecksumInfo.SHA1))
                {
                    totalBytesToHash += dataFile.Length;
                    updateRequired = true;
                }

                if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC &&
                    string.IsNullOrWhiteSpace(fileChecksumInfo.MD5))
                {
                    totalBytesToHash += dataFile.Length;
                    updateRequired = true;
                }

                if (updateRequired)
                {
                    datasetCountToProcess++;
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

                    var checksumFileUpdater = GetChecksumUpdater(checksumData, targetDirectory);

                    foreach (var datasetFile in dataset.TargetDirectoryFiles)
                    {
                        checksumFileUpdater.AddDataFile(datasetFile);
                    }
                }

                if (checksumData.Count == 0)
                    return true;

                var progressChunkSize = 100 / (float)checksumData.Count;

                var itemsProcessed = 0;
                var successCount = 0;

                foreach (var item in checksumData)
                {
                    var progressAtStart = itemsProcessed * progressChunkSize;
                    var progressAtEnd = (itemsProcessed + 1) * progressChunkSize;

                    var updateSuccess = CreateOrUpdateChecksumFile(item.Value, progressAtStart, progressAtEnd);
                    if (updateSuccess)
                        successCount++;

                    itemsProcessed++;
                }

                return (successCount == checksumData.Count);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateChecksumFiles", ex);
                return false;
            }
        }

        private bool CreateOrUpdateChecksumFile(ChecksumFileUpdater checksumFileUpdater, float progressAtStart, float progressAtEnd)
        {
            try
            {
                checksumFileUpdater.LoadExistingChecksumFile();

                var success = ComputeFileChecksums(checksumFileUpdater, progressAtStart, progressAtEnd);

                {
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
            if (checksumData.TryGetValue(targetDirectory.FullName, out var checksumFileUpdater))
            {
                return checksumFileUpdater;
            }

            var newUpdater = new ChecksumFileUpdater(targetDirectory, Options.ChecksumFileMode);
            checksumData.Add(targetDirectory.FullName, newUpdater);

            return newUpdater;
        }

        private FileChecksumInfo GetFileChecksumInfo(ChecksumFileUpdater checksumFileUpdater, FileSystemInfo dataFile)
        {
            if (checksumFileUpdater.DataFileChecksums.TryGetValue(dataFile.Name, out var fileChecksumInfo))
            {
                return fileChecksumInfo;
            }

            var newChecksumInfo = new FileChecksumInfo(dataFile.Name);
            checksumFileUpdater.DataFileChecksums.Add(dataFile.Name, newChecksumInfo);

            return newChecksumInfo;
        }
    }
}
