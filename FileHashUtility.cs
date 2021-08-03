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
        // Ignore Spelling: cmd, gsutil, Foreach, cp

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
        /// Constructor
        /// </summary>
        /// <param name="options">Options</param>
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

        private void AlignUploadCommands(IList<string> uploadCommands)
        {
            var largestIndex = 0;
            foreach (var item in uploadCommands)
            {
                var gsIndex = item.IndexOf(Options.RemoteUploadBaseURL, StringComparison.Ordinal);
                if (gsIndex > largestIndex)
                    largestIndex = gsIndex;
            }

            if (largestIndex <= 0)
                return;

            // Create a format string similar to {0,-245}{1}
            var formatString = "{0,-" + largestIndex + "}{1}";

            for (var i = 0; i < uploadCommands.Count; i++)
            {
                var gsIndex = uploadCommands[i].IndexOf(Options.RemoteUploadBaseURL, StringComparison.Ordinal);
                if (gsIndex < 0)
                    continue;

                var leftHalf = uploadCommands[i].Substring(0, gsIndex);
                var rightHalf = uploadCommands[i].Substring(gsIndex);

                uploadCommands[i] = string.Format(formatString, leftHalf, rightHalf);
            }
        }

        /// <summary>
        /// Look for any text files in the specified directory, recursively searching subdirectories
        /// For any not present in processedFiles, append upload commands to the batch file and to the checksum file
        /// Also append the checksum file to the batch file
        /// </summary>
        /// <param name="checksumFileUpdater">Checksum file updater (only used if ChecksumFileMode is MoTrPAC</param>
        /// <param name="baseOutputDirectoryPath">Base output directory path</param>
        /// <param name="uploadCommands">List of commands for the batch file</param>
        /// <param name="processedFiles">List of processed files</param>
        /// <param name="directory">Directory</param>
        /// <returns>True if no errors, false if checksum mismatch</returns>
        private bool AppendTextFilesToOutputFiles(
            ChecksumFileUpdater checksumFileUpdater,
            string baseOutputDirectoryPath,
            ICollection<string> uploadCommands,
            ISet<string> processedFiles,
            DirectoryInfo directory)
        {
            var filesToAdd = directory.GetFiles("*.txt", SearchOption.AllDirectories).ToList();

            var newFiles = new List<FileInfo>();

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var dataFile in filesToAdd)
            {
                if (processedFiles.Contains(dataFile.FullName))
                {
                    continue;
                }

                AppendUploadCommand(uploadCommands, processedFiles, dataFile);
                newFiles.Add(dataFile);
            }

            var checksumFilePath = checksumFileUpdater.GetChecksumFilePath();

            if (string.IsNullOrWhiteSpace(checksumFilePath) || Options.ChecksumFileMode != DatasetRetrieverOptions.ChecksumFileType.MoTrPAC)
                return true;

            // Also append the text files to the checksum file, but only if not yet present
            using var writer = new StreamWriter(new FileStream(checksumFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));

            var dataValues = new List<string>();
            foreach (var item in newFiles)
            {
                string relativeFilePath;
                if (item.FullName.StartsWith(baseOutputDirectoryPath, StringComparison.OrdinalIgnoreCase))
                {
                    relativeFilePath = GetRelativeFilePath(item, baseOutputDirectoryPath);
                }
                else
                {
                    relativeFilePath = item.Name;
                }

                var md5Sum = ComputeChecksumMD5(item, out _);
                var sha1Sum = ComputeChecksumSHA1(item);

                dataValues.Clear();

                var relativePathToStore = ChecksumFileUpdater.UpdatePathSeparators(relativeFilePath);

                if (checksumFileUpdater.DataFileChecksums.TryGetValue(relativePathToStore, out var existingChecksum))
                {
                    if (existingChecksum.SHA1.Equals(sha1Sum))
                        continue;

                    OnWarningEvent("Existing checksum value does not match the new checksum value for file " + relativePathToStore);
                    OnWarningEvent(string.Format("Old: {0}", existingChecksum.SHA1));
                    OnWarningEvent(string.Format("New: {0}", sha1Sum));
                    return false;
                }

                var checksumInfo = new FileChecksumInfo(relativePathToStore)
                {
                    MD5 = md5Sum,
                    SHA1 = sha1Sum
                };

                checksumFileUpdater.DataFileChecksums.Add(relativePathToStore, checksumInfo);

                dataValues.Add(relativePathToStore);
                dataValues.Add(md5Sum);
                dataValues.Add(sha1Sum);

                writer.WriteLine(string.Join(",", dataValues));
            }

            return true;
        }

        private void AppendUploadCommand(ICollection<string> uploadCommands, ISet<string> processedFiles, FileInfo dataFile, string md5Base64 = "")
        {
            var remoteUrl = GenerateRemoteUrl(dataFile.FullName);

            string uploadCommand;

            var sourceFileToUse = GetLocalOrRemoteFile(dataFile);
            if (!sourceFileToUse.Exists)
            {
                ConsoleMsgUtils.ShowWarning(
                    "Data file not found; adding to batch file anyway, but upload error will occur: \n  " + sourceFileToUse.FullName);
            }

            // Use "call cmd /c" because gsutil returns a non-zero exit code, and that will terminate a batch file
            const string gsutilCommand = "call cmd /c gsutil";

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (string.IsNullOrWhiteSpace(md5Base64))
            {
                uploadCommand = string.Format("{0} cp {1} {2}", gsutilCommand, sourceFileToUse.FullName, remoteUrl);
            }
            else
            {
                uploadCommand = string.Format("{0} -h Content-MD5:{1} cp {2} {3}", gsutilCommand, md5Base64, sourceFileToUse.FullName, remoteUrl);
            }

            uploadCommands.Add(uploadCommand);
            processedFiles.Add(dataFile.FullName);
        }

        private string ComputeChecksumMD5(FileSystemInfo dataFile, out string base64MD5)
        {
            return HashUtilities.ComputeFileHashMD5(dataFile.FullName, out base64MD5);
        }

        private string ComputeChecksumSHA1(FileSystemInfo dataFile)
        {
            return HashUtilities.ComputeFileHashSha1(dataFile.FullName);
        }

        /// <summary>
        /// Compute checksums for files in checksumFileUpdater.DataFiles
        /// </summary>
        /// <param name="checksumFileUpdater">Checksum file updater</param>
        /// <param name="baseOutputDirectoryPath">Checksum file updater</param>
        /// <param name="progressAtStart">Progress at start</param>
        /// <param name="progressAtEnd">Progress at the end</param>
        private bool ComputeFileChecksums(
            ChecksumFileUpdater checksumFileUpdater,
            string baseOutputDirectoryPath,
            float progressAtStart,
            float progressAtEnd)
        {
            try
            {
                var totalBytesToHash = ComputeTotalBytesToHash(checksumFileUpdater, baseOutputDirectoryPath, out var datasetCountToProcess);

                long totalBytesHashed = 0;
                var lastProgressTime = DateTime.UtcNow;

                var checksumFile = new FileInfo(checksumFileUpdater.ChecksumFilePath);

                if (datasetCountToProcess == 0)
                {
                    if (checksumFile.Exists && checksumFile.Length > 0)
                    {
                        Console.WriteLine();
                        OnStatusEvent(string.Format(
                            "Checksum values are already up to date for {0} in {1}",
                            DMSDatasetRetriever.GetCountWithUnits(checksumFileUpdater.DataFiles.Count, "dataset", "datasets"),
                            Path.GetFileName(checksumFileUpdater.ChecksumFilePath)));

                        return true;
                    }

                    Console.WriteLine();
                    OnStatusEvent(string.Format(
                        "Loaded checksum values from an existing file, but need to create {0} for {1}",
                        PathUtils.CompactPathString(checksumFileUpdater.ChecksumFilePath, 80),
                        DMSDatasetRetriever.GetCountWithUnits(checksumFileUpdater.DataFiles.Count, "dataset", "datasets")
                    ));
                }

                Console.WriteLine();
                var action = Options.PreviewMode ? "Preview compute" : "Computing";

                // Only append the number of bytes to process if totalBytesToHash is non-zero
                var bytesToProcess = totalBytesToHash > 0 ?
                                         string.Format("; {0} to process", FileTools.BytesToHumanReadable(totalBytesToHash)) :
                                         string.Empty;

                // Example messages:
                // Preview compute checksum values for 1 dataset in CheckSumFileName
                // Computing checksum values for 3 datasets in CheckSumFileName; 12.3 GB to process
                OnStatusEvent(string.Format(
                    "{0} checksum values for {1} in {2}{3}",
                    action,
                    DMSDatasetRetriever.GetCountWithUnits(datasetCountToProcess, "dataset", "datasets"),
                    Path.GetFileName(checksumFileUpdater.ChecksumFilePath),
                    bytesToProcess));

                foreach (var dataFile in checksumFileUpdater.DataFiles)
                {
                    var fileChecksumInfo = GetFileChecksumInfo(checksumFileUpdater, dataFile, baseOutputDirectoryPath);

                    var fileToHash = GetLocalOrRemoteFile(dataFile);

                    var computeSHA1 = string.IsNullOrWhiteSpace(fileChecksumInfo.SHA1);

                    var computeMD5 = string.IsNullOrWhiteSpace(fileChecksumInfo.MD5) &&
                                     Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC;

                    if (Options.PreviewMode)
                    {
                        if (computeMD5 && computeSHA1)
                        {
                            OnDebugEvent("Compute SHA-1 and MD5 hashes: " + fileToHash.Name);
                        }
                        else if (computeMD5)
                        {
                            OnDebugEvent("Compute MD5 hash: " + fileToHash.Name);
                        }
                        else if (computeSHA1)
                        {
                            OnDebugEvent("Compute SHA-1 hash: " + fileToHash.Name);
                        }
                        continue;
                    }

                    if (computeSHA1)
                    {
                        if (!fileToHash.Exists)
                        {
                            OnWarningEvent("File not found; cannot compute the SHA-1 hash for " + fileToHash.FullName);
                            continue;
                        }

                        OnDebugEvent("Computing SHA-1 hash: " + fileToHash.Name);
                        fileChecksumInfo.SHA1 = ComputeChecksumSHA1(fileToHash);
                        totalBytesHashed += fileToHash.Length;
                    }

                    if (computeMD5)
                    {
                        if (!fileToHash.Exists)
                        {
                            OnWarningEvent("File not found; cannot compute the MD5 hash for " + fileToHash.FullName);
                            continue;
                        }

                        OnDebugEvent("Computing MD5 hash:   " + fileToHash.Name);
                        fileChecksumInfo.MD5 = ComputeChecksumMD5(fileToHash, out var base64MD5);
                        fileChecksumInfo.MD5_Base64 = base64MD5;

                        totalBytesHashed += fileToHash.Length;
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

        private long ComputeTotalBytesToHash(
            ChecksumFileUpdater checksumFileUpdater,
            string baseOutputDirectoryPath,
            out int datasetCountToProcess)
        {
            long totalBytesToHash = 0;
            datasetCountToProcess = 0;

            foreach (var dataFile in checksumFileUpdater.DataFiles)
            {
                var dataFileToHash = GetLocalOrRemoteFile(dataFile);

                var fileChecksumInfo = GetFileChecksumInfo(checksumFileUpdater, dataFile, baseOutputDirectoryPath);
                var updateRequired = false;

                var fileSizeBytes = dataFileToHash.Exists ? dataFileToHash.Length : 0L;

                if (string.IsNullOrWhiteSpace(fileChecksumInfo.SHA1))
                {
                    totalBytesToHash += fileSizeBytes;
                    updateRequired = true;
                }

                if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC &&
                    string.IsNullOrWhiteSpace(fileChecksumInfo.MD5))
                {
                    totalBytesToHash += fileSizeBytes;
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
        /// <param name="datasetList">Dataset list</param>
        /// <param name="baseOutputDirectoryPath">
        /// Base output directory (used when checksumFileMode is ChecksumFileType.MoTrPAC)
        /// </param>
        public bool CreateChecksumFiles(IEnumerable<DatasetInfo> datasetList, string baseOutputDirectoryPath)
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

                    DirectoryInfo checksumFileDirectory;
                    if (Options.ChecksumFileMode == DatasetRetrieverOptions.ChecksumFileType.MoTrPAC && baseOutputDirectoryPath.Length > 0)
                    {
                        checksumFileDirectory = new DirectoryInfo(baseOutputDirectoryPath);
                    }
                    else
                    {
                        checksumFileDirectory = dataset.TargetDirectoryFiles.First().Directory;
                    }

                    var checksumFileUpdater = GetChecksumUpdater(checksumData, checksumFileDirectory, baseOutputDirectoryPath);

                    foreach (var datasetFile in dataset.TargetDirectoryFiles)
                    {
                        if (datasetFile.Name.EndsWith(FileCopyUtility.LINK_FILE_SUFFIX))
                        {
                            var localDatasetFile = new FileInfo(GetPathWithoutLinkFileSuffix(datasetFile));
                            checksumFileUpdater.AddDataFile(localDatasetFile);
                        }
                        else
                        {
                            checksumFileUpdater.AddDataFile(datasetFile);
                        }
                    }
                }

                if (checksumData.Count == 0)
                    return true;

                Console.WriteLine();

                var progressChunkSize = 100 / (float)checksumData.Count;

                var itemsProcessed = 0;
                var successCount = 0;

                foreach (var item in checksumData)
                {
                    var progressAtStart = itemsProcessed * progressChunkSize;
                    var progressAtEnd = (itemsProcessed + 1) * progressChunkSize;

                    var updateSuccess = CreateOrUpdateChecksumFile(item.Value, baseOutputDirectoryPath, progressAtStart, progressAtEnd);
                    if (updateSuccess)
                        successCount++;

                    itemsProcessed++;
                }

                var checksumSuccessOverall = (successCount == checksumData.Count);

                var batchFileSuccess = CreateUploadBatchFile(checksumData, baseOutputDirectoryPath);

                return checksumSuccessOverall && batchFileSuccess;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateChecksumFiles", ex);
                return false;
            }
        }

        private bool CreateOrUpdateChecksumFile(
            ChecksumFileUpdater checksumFileUpdater,
            string baseOutputDirectoryPath,
            float progressAtStart,
            float progressAtEnd)
        {
            try
            {
                checksumFileUpdater.LoadExistingChecksumFile();

                var success = ComputeFileChecksums(checksumFileUpdater, baseOutputDirectoryPath, progressAtStart, progressAtEnd);

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

        private bool CreateUploadBatchFile(
            IReadOnlyDictionary<string, ChecksumFileUpdater> checksumData,
            string baseOutputDirectoryPath)
        {
            if (Options.ChecksumFileMode != DatasetRetrieverOptions.ChecksumFileType.MoTrPAC)
            {
                // Upload batch file creation is not supported for this checksum file type
                // Nothing to do
                return true;
            }

            if (Options.VerboseMode)
            {
                Console.WriteLine();
            }

            try
            {
                var batchFileNameSuffix = string.IsNullOrWhiteSpace(Options.DatasetInfoFilePath) ?
                                              string.Empty :
                                              string.Format("_{0}", Path.GetFileNameWithoutExtension(Options.DatasetInfoFilePath));

                var batchFileName = string.Format("UploadFiles_{0:yyyy-MM-dd}{1}.bat", DateTime.Now, batchFileNameSuffix);

                string uploadBatchFilePath;

                if (string.IsNullOrWhiteSpace(Options.RemoteUploadBatchFilePath))
                {
                    if (Options.VerboseMode)
                    {
                        OnStatusEvent("Creating default-named batch file in the OutputDirectoryPath: " + Options.OutputDirectoryPath);
                    }

                    uploadBatchFilePath = Path.Combine(Options.OutputDirectoryPath, batchFileName);
                }
                else if (Path.IsPathRooted(Options.RemoteUploadBatchFilePath))
                {
                    if (Options.VerboseMode)
                    {
                        OnStatusEvent("RemoteUploadBatchFilePath is rooted; treating as an absolute path");
                    }

                    if (Options.RemoteUploadBatchFilePath.Trim().EndsWith(".bat", StringComparison.OrdinalIgnoreCase))
                    {
                        uploadBatchFilePath = Options.RemoteUploadBatchFilePath;
                    }
                    else
                    {
                        uploadBatchFilePath = Path.Combine(Options.RemoteUploadBatchFilePath, batchFileName);
                    }
                }
                else
                {
                    if (Options.RemoteUploadBatchFilePath.Trim().EndsWith(".bat", StringComparison.OrdinalIgnoreCase))
                    {
                        if (Options.VerboseMode)
                        {
                            OnStatusEvent(string.Format(
                                "RemoteUploadBatchFilePath is not rooted; appending file {0} to {1}",
                                Options.RemoteUploadBatchFilePath, Options.OutputDirectoryPath));
                        }

                        uploadBatchFilePath = Path.Combine(Options.OutputDirectoryPath, Options.RemoteUploadBatchFilePath);
                    }
                    else
                    {
                        var targetDirectoryPath = Path.Combine(Options.OutputDirectoryPath, Options.RemoteUploadBatchFilePath);

                        if (Options.VerboseMode)
                        {
                            OnStatusEvent(string.Format(
                                "RemoteUploadBatchFilePath is not rooted; appending file {0} to {1}",
                                batchFileName, targetDirectoryPath));
                        }

                        uploadBatchFilePath = Path.Combine(targetDirectoryPath, batchFileName);
                    }
                }

                Console.WriteLine();

                if (Options.PreviewMode)
                {
                    OnStatusEvent("Would create " + uploadBatchFilePath);
                    if (!Path.IsPathRooted(uploadBatchFilePath))
                    {
                        var batchFileInfo = new FileInfo(uploadBatchFilePath);
                        OnDebugEvent("Full path: " + PathUtils.CompactPathString(batchFileInfo.FullName, 100));
                    }

                    return true;
                }

                OnStatusEvent("Creating " + uploadBatchFilePath);

                // List of files added to the batch file
                var processedFiles = new SortedSet<string>();

                // List of parent directories that should be checked recursively for additional text files
                // The number of levels up is defined in Options
                var parentDirectoryPaths = new SortedSet<string>();

                // Use a list to cache the data that will be written to the batch file
                // Once the list is complete, create/update the batch file
                var uploadCommands = new List<string>();

                foreach (var item in checksumData)
                {
                    var checksumFileUpdater = item.Value;

                    foreach (var dataFile in checksumFileUpdater.DataFiles)
                    {
                        if (processedFiles.Contains(dataFile.FullName))
                        {
                            OnWarningEvent("Duplicate file found; skipping " + dataFile.FullName);
                            continue;
                        }

                        var fileChecksumInfo = GetFileChecksumInfo(checksumFileUpdater, dataFile, baseOutputDirectoryPath);
                        var md5Base64 = GetBase64MD5(fileChecksumInfo);

                        AppendUploadCommand(uploadCommands, processedFiles, dataFile, md5Base64);
                    }

                    var success = AppendTextFilesToOutputFiles(
                        checksumFileUpdater, baseOutputDirectoryPath,
                        uploadCommands, processedFiles,
                        checksumFileUpdater.ChecksumFileDirectory);

                    if (!success)
                    {
                        OnWarningEvent("AppendTextFilesToOutputFiles returned false; aborting");
                        return false;
                    }

                    if (Options.ParentDirectoryDepth > 0)
                    {
                        var parentDirectory = item.Value.ChecksumFileDirectory.Parent;
                        for (var i = 2; i <= Options.ParentDirectoryDepth; i++)
                        {
                            if (parentDirectory == null)
                                break;

                            parentDirectory = parentDirectory.Parent;
                        }

                        if (parentDirectory != null && !parentDirectoryPaths.Contains(parentDirectory.FullName))
                        {
                            parentDirectoryPaths.Add(parentDirectory.FullName);
                        }
                    }

                    uploadCommands.Add(string.Empty);
                }

                if (checksumData.Count > 0)
                {
                    var checksumFileUpdater = checksumData[checksumData.Keys.First()];

                    // Step through parentDirectoryPaths and look for any unprocessed text files in subdirectories
                    foreach (var parentDirectory in parentDirectoryPaths)
                    {
                        AppendTextFilesToOutputFiles(
                            checksumFileUpdater, baseOutputDirectoryPath,
                            uploadCommands, processedFiles,
                            new DirectoryInfo(parentDirectory));
                    }

                    var checksumFile = new FileInfo(checksumFileUpdater.GetChecksumFilePath());
                    AppendUploadCommand(uploadCommands, processedFiles, checksumFile);
                }
                else
                {
                    OnWarningEvent("The checksumData dictionary is empty in CreateUploadBatchFile; not creating a checksum file");
                }

                // Format the commands to align on gs://
                AlignUploadCommands(uploadCommands);

                using var writer = new StreamWriter(new FileStream(uploadBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                foreach (var item in uploadCommands)
                {
                    writer.WriteLine(item);
                }

                Console.WriteLine();

                // Example: 12 file upload commands written to the batch file
                OnStatusEvent(string.Format(
                    "{0} written to the batch file",
                    DMSDatasetRetriever.GetCountWithUnits(processedFiles.Count, "file upload command", "file upload commands")));

                Console.WriteLine();
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateUploadBatchFile", ex);
                return false;
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

            return Convert.ToBase64String(byteArray.ToArray());
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

                if (fullFilePath.Substring(1, 2).Equals(@":\"))
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

            return remoteUrlBase + fullFilePath.Substring(charIndex + RemoteUploadURLDirectoriesToMatch.Length).Replace('\\', '/');
        }

        private ChecksumFileUpdater GetChecksumUpdater(
            IDictionary<string, ChecksumFileUpdater> checksumData,
            DirectoryInfo checksumFileDirectory,
            string baseOutputDirectoryPath)
        {
            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (checksumData.TryGetValue(checksumFileDirectory.FullName, out var checksumFileUpdater))
            {
                return checksumFileUpdater;
            }

            var newUpdater = new ChecksumFileUpdater(checksumFileDirectory, Options.ChecksumFileMode, baseOutputDirectoryPath, Options.ChecksumFileNameDate);
            checksumData.Add(checksumFileDirectory.FullName, newUpdater);

            return newUpdater;
        }

        private FileChecksumInfo GetFileChecksumInfo(
            ChecksumFileUpdater checksumFileUpdater,
            FileSystemInfo dataFile,
            string baseOutputDirectoryPath)
        {
            var datasetFilePath = GetPathWithoutLinkFileSuffix(dataFile);

            // First look for a match on filename alone
            if (checksumFileUpdater.DataFileChecksums.TryGetValue(Path.GetFileName(datasetFilePath), out var fileChecksumInfo))
            {
                return fileChecksumInfo;
            }

            string relativeFilePath;
            if (dataFile.FullName.StartsWith(baseOutputDirectoryPath, StringComparison.OrdinalIgnoreCase))
            {
                relativeFilePath = GetRelativeFilePath(dataFile, baseOutputDirectoryPath);
                // Look for the relative path of the file

                if (checksumFileUpdater.DataFileChecksums.TryGetValue(relativeFilePath, out var fileChecksumInfo2))
                {
                    return fileChecksumInfo2;
                }

                // Try Linux-style directory separators
                if (checksumFileUpdater.DataFileChecksums.TryGetValue(ChecksumFileUpdater.UpdatePathSeparators(relativeFilePath), out var fileChecksumInfo3))
                {
                    return fileChecksumInfo3;
                }
            }
            else
            {
                relativeFilePath = Path.GetFileName(datasetFilePath);
            }

            var newChecksumInfo = new FileChecksumInfo(relativeFilePath);
            checksumFileUpdater.DataFileChecksums.Add(relativeFilePath, newChecksumInfo);

            return newChecksumInfo;
        }

        private FileInfo GetLocalOrRemoteFile(FileInfo dataFile)
        {
            if (dataFile.Exists)
            {
                return dataFile;
            }

            var linkFile = new FileInfo(dataFile.FullName + FileCopyUtility.LINK_FILE_SUFFIX);
            if (linkFile.Exists)
            {
                var remoteFilePath = GetRemotePathFromLinkFile(linkFile);

                if (!string.IsNullOrWhiteSpace(remoteFilePath))
                {
                    return new FileInfo(remoteFilePath);
                }
            }

            return dataFile;
        }

        private string GetPathWithoutLinkFileSuffix(FileSystemInfo datasetFile)
        {
            if (datasetFile.FullName.EndsWith(FileCopyUtility.LINK_FILE_SUFFIX))
                return datasetFile.FullName.Substring(0, datasetFile.FullName.Length - FileCopyUtility.LINK_FILE_SUFFIX.Length);

            return datasetFile.FullName;
        }

        private string GetRelativeFilePath(FileSystemInfo dataFile, string baseOutputDirectoryPath)
        {
            var baseOutputDirectoryName = Path.GetFileName(baseOutputDirectoryPath);

            // Look for the relative path of the file
            return Path.Combine(baseOutputDirectoryName, dataFile.FullName.Substring(baseOutputDirectoryPath.Length).TrimStart('\\'));
        }

        private string GetRemotePathFromLinkFile(FileSystemInfo linkFile)
        {
            using var reader = new StreamReader(new FileStream(linkFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            return !reader.EndOfStream ? reader.ReadLine() : string.Empty;
        }
    }
}
