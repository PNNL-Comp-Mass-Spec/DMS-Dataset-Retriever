using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;
using PRISM.FileProcessor;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// Utility for computing MD5 and SHA-1 checksums
    /// </summary>
    internal class FileHashUtility : EventNotifier
    {
        #region "Properties"

        /// <summary>
        /// Retrieval options
        /// </summary>
        private DatasetRetrieverOptions Options { get; }

        /// <summary>
        /// Directory names after the server or bucket name in the remote upload URL
        /// Names will be separated by the local computer's directory separator character
        /// </summary>
        private string RemoteUploadURLDirectoriesToMatch { get; }

        #endregion

        /// <summary>
        /// Look for any text files in the specified directory, recursively searching subdirectories
        /// For any not present in processedFiles, append upload commands to the batch file
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="processedFiles"></param>
        /// <param name="directory"></param>
        private void AppendTextFilesToBatchFile(TextWriter writer, ISet<string> processedFiles, DirectoryInfo directory)
        {
            var textFiles = directory.GetFiles("*.txt", SearchOption.AllDirectories).ToList();
            if (textFiles.Count == 0)
            {
                return;
            }

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var dataFile in textFiles)
            {
                if (processedFiles.Contains(dataFile.FullName))
                {
                    continue;
                }

                AppendUploadCommand(writer, processedFiles, dataFile);
            }
        }

        private void AppendUploadCommand(TextWriter writer, ISet<string> processedFiles, FileSystemInfo dataFile, string md5Base64 = "")
        {
            var remoteUrl = GenerateRemoteUrl(dataFile.FullName);

            string uploadCommand;

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (string.IsNullOrWhiteSpace(md5Base64))
            {
                uploadCommand = string.Format("gsutil cp {0} {1}", dataFile.FullName, remoteUrl);
            }
            else
            {
                uploadCommand = string.Format("gsutil -h Content-MD5:{0} cp {1} {2}", md5Base64, dataFile.FullName, remoteUrl);
            }

            writer.WriteLine(uploadCommand);
            processedFiles.Add(dataFile.FullName);
        }

        private string ComputeChecksumMD5(FileSystemInfo dataFile, out string base64MD5)
        {
            var md5 = HashUtilities.ComputeFileHashMD5(dataFile.FullName, out base64MD5);
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
        private bool ComputeFileChecksums(ChecksumFileUpdater checksumFileUpdater, float progressAtStart, float progressAtEnd)
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
                        fileChecksumInfo.MD5 = ComputeChecksumMD5(dataFile, out var base64MD5);
                        fileChecksumInfo.MD5_Base64 = base64MD5;

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
            if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.None)
            {
                OnWarningEvent("FileHashUtility.CreateChecksumFiles called when Options.ChecksumFileMode is ChecksumFileType.None; nothing to do");
                return true;
            }

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

                var checksumSuccessOverall = (successCount == checksumData.Count);

                CreateUploadBatchFile(checksumData);

                return checksumSuccessOverall;
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

                if (success && !Options.PreviewMode)
                {
                    checksumFileUpdater.WriteChecksumFile();
                }

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateOrUpdateChecksumFile", ex);
                return false;
            }
        }

        private void CreateUploadBatchFile(IReadOnlyDictionary<string, ChecksumFileUpdater> checksumData)
        {
            if (Options.ChecksumFileMode != DatasetRetrieverOptions.ChecksumFileType.MoTrPAC)
            {
                // Upload batch file creation is not supported for this checksum file type
                // Nothing to do
            }

            try
            {
                var batchFileName = string.Format("UploadFiles_{0:yyyy-MM-dd}.bat", DateTime.Now);
                var uploadBatchFilePath = Path.Combine(Options.OutputDirectoryPath, batchFileName);

                if (Options.PreviewMode)
                {
                    OnStatusEvent("Would create " + uploadBatchFilePath);
                    return;
                }

                Console.WriteLine();
                OnStatusEvent("Creating " + uploadBatchFilePath);

                // List of files added to the batch file
                var processedFiles = new SortedSet<string>();

                // List of parent directories that should be checked recursively for additional text files
                // The number of levels up is defined in Options
                var parentDirectoryPaths = new SortedSet<string>();

                using (var writer = new StreamWriter(new FileStream(uploadBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var item in checksumData)
                    {
                        foreach (var dataFile in item.Value.DataFiles)
                        {
                            if (processedFiles.Contains(dataFile.FullName))
                            {
                                OnWarningEvent("Duplicate file found; skipping " + dataFile.FullName);
                                continue;
                            }

                            var fileChecksumInfo = GetFileChecksumInfo(item.Value, dataFile);
                            var md5Base64 = GetBase64MD5(fileChecksumInfo);

                            AppendUploadCommand(writer, processedFiles, dataFile, md5Base64);
                        }

                        AppendTextFilesToBatchFile(writer, processedFiles, item.Value.DataFileDirectory);

                        if (Options.ParentDirectoryDepth > 0)
                        {
                            var parentDirectory = item.Value.DataFileDirectory.Parent;
                            for (var i = 2; i <= Options.ParentDirectoryDepth; i++)
                            {
                                if (parentDirectory == null)
                                    break;

                                parentDirectory = parentDirectory.Parent;
                            }

                            if (parentDirectory != null && !parentDirectoryPaths.Contains(parentDirectory.FullName))
                                parentDirectoryPaths.Add(parentDirectory.FullName);
                        }

                        writer.WriteLine();
                    }

                    // Step through parentDirectoryPaths and look for any unprocessed text files in subdirectories
                    foreach (var parentDirectory in parentDirectoryPaths)
                    {
                        AppendTextFilesToBatchFile(writer, processedFiles, new DirectoryInfo(parentDirectory));
                    }
                }

                Console.WriteLine();
                OnStatusEvent(string.Format("{0} file upload commands written", processedFiles.Count));
                Console.WriteLine();

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateUploadBatchFile", ex);
            }

        }

        private string GetBase64MD5(FileChecksumInfo fileChecksumInfo)
        {
            if (!string.IsNullOrWhiteSpace(fileChecksumInfo.MD5_Base64))
                return fileChecksumInfo.MD5_Base64;

            if (string.IsNullOrWhiteSpace(fileChecksumInfo.MD5))
                return string.Empty;

            var byteArray = new List<byte>();
            var md5Hash = fileChecksumInfo.MD5;

            for (var i = 0; i < md5Hash.Length; i += 2)
            {
                var nextByte = Convert.ToByte(md5Hash.Substring(i, 2), 16);
                byteArray.Add(nextByte);
            }

            var base64MD5 = Convert.ToBase64String(byteArray.ToArray());

            return base64MD5;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public FileHashUtility(DatasetRetrieverOptions options)
        {
            Options = options;

            if (string.IsNullOrWhiteSpace(Options.RemoteUploadBaseURL))
            {
                RemoteUploadURLDirectoriesToMatch = string.Empty;
                return;
            }

            var directoryMatcher = new Regex("^[a-z]+://[^/]+/(?<Directories>.+)", RegexOptions.IgnoreCase);

            var match = directoryMatcher.Match(Options.RemoteUploadBaseURL);
            if (!match.Success)
            {
                throw new Exception(
                    "RemoteUploadBaseURL is not of the form " +
                    "gs://bucket-name/DirectoryA/DirectoryB or " +
                    "http://server/DirectoryA/DirectoryB or similar; " +
                    "it is " +
                    Options.RemoteUploadBaseURL);
            }

            var uploadUrlDirectories = new List<string>();

            foreach (var pathPart in match.Groups["Directories"].Value.Split('/'))
            {
                if (string.IsNullOrEmpty(pathPart))
                    continue;
                uploadUrlDirectories.Add(pathPart);
            }

            RemoteUploadURLDirectoriesToMatch = string.Join(Path.DirectorySeparatorChar.ToString(), uploadUrlDirectories) + Path.DirectorySeparatorChar;
        }

        private string GenerateRemoteUrl(string fullFilePath)
        {
            var charIndex = fullFilePath.LastIndexOf(RemoteUploadURLDirectoriesToMatch, StringComparison.OrdinalIgnoreCase);
            if (charIndex < 0)
            {
                OnWarningEvent(string.Format(
                    "Could not find '{0}' in the local file path; cannot generate the remote URL for \n  {1}",
                    RemoteUploadURLDirectoriesToMatch,
                    fullFilePath));

                if (fullFilePath.Length < 3)
                    return fullFilePath;

                if (fullFilePath.Substring(1,2).Equals(@":\"))
                    return fullFilePath.Substring(3).Replace('\\', '/');

                var slashIndex = fullFilePath.IndexOf('\\', 3);
                if (slashIndex > 0 && slashIndex < fullFilePath.Length - 1)
                    return fullFilePath.Substring(slashIndex + 1).Replace('\\', '/');

                return fullFilePath.Replace('\\', '/');
            }

            string remoteUrlBase;
            if (Options.RemoteUploadBaseURL.EndsWith("/"))
                remoteUrlBase = Options.RemoteUploadBaseURL;
            else
                remoteUrlBase = Options.RemoteUploadBaseURL + "/";

            var remoteUrl = remoteUrlBase + fullFilePath.Substring(charIndex + RemoteUploadURLDirectoriesToMatch.Length).Replace('\\', '/');

            return remoteUrl;
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
