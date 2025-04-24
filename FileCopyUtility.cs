using System;
using System.Collections.Generic;
using System.IO;
using PRISM;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// Utility for copying dataset files to the specified directory
    /// </summary>
    internal class FileCopyUtility : EventNotifier
    {
        /// <summary>
        /// Dataset link file suffix
        /// </summary>
        public const string LINK_FILE_SUFFIX = ".dslink";

        /// <summary>
        /// Retrieval options
        /// </summary>
        private DatasetRetrieverOptions Options { get; }

        /// <summary>
        /// Total bytes to copy
        /// </summary>
        public long TotalBytesToCopy { get; private set; }

        /// <summary>
        /// Bytes that have been copied so far
        /// </summary>
        public long BytesCopied { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options">Options</param>
        public FileCopyUtility(DatasetRetrieverOptions options)
        {
            Options = options;
        }

        private void ComputeTotalBytesAddDirectory(
            DatasetInfo datasetInfo,
            FileSystemInfo outputDirectory,
            DatasetFileOrDirectory sourceDirectoryInfo)
        {
            var sourceDirectory = new DirectoryInfo(sourceDirectoryInfo.SourcePath);

            if (!sourceDirectory.Exists)
                return;

            foreach (var sourceFile in sourceDirectory.GetFiles())
            {
                // RelativeTargetPath should have the target directory name, possibly preceded by a subdirectory name
                var relativeTargetPath = Path.Combine(sourceDirectoryInfo.RelativeTargetPath, sourceFile.Name);

                var sourceFileInfo = new DatasetFileOrDirectory(
                    sourceDirectoryInfo.DatasetInfo,
                    sourceFile,
                    relativeTargetPath,
                    sourceDirectoryInfo.MyEMSLDownloader);

                ComputeTotalBytesAddFileIfMissing(outputDirectory, sourceFileInfo);
            }

            foreach (var subdirectory in sourceDirectory.GetDirectories())
            {
                // RelativeTargetPath should have the target directory name, possibly preceded by a subdirectory name
                var relativeTargetPath = Path.Combine(sourceDirectoryInfo.RelativeTargetPath, subdirectory.Name);

                var subDirectoryInfo = new DatasetFileOrDirectory(
                    datasetInfo, subdirectory, relativeTargetPath,
                    sourceDirectoryInfo.MyEMSLDownloader);

                ComputeTotalBytesAddDirectory(datasetInfo, outputDirectory, subDirectoryInfo);
            }
        }

        private void ComputeTotalBytesAddFileIfMissing(FileSystemInfo outputDirectory, DatasetFileOrDirectory sourceItem)
        {
            var sourceFile = new FileInfo(sourceItem.SourcePath);

            if (!sourceFile.Exists)
                return;

            // RelativeTargetPath should have the target file name, possibly preceded by a subdirectory name
            var targetFile = new FileInfo(Path.Combine(outputDirectory.FullName, sourceItem.RelativeTargetPath));

            if (!targetFile.Exists)
            {
                TotalBytesToCopy += sourceFile.Length;
            }
        }

        private void ComputeTotalBytesToCopy(
            Dictionary<DatasetInfo, List<DatasetFileOrDirectory>> sourceFilesByDataset,
            FileSystemInfo outputDirectory,
            out int datasetCountToCopy)
        {
            TotalBytesToCopy = 0;
            BytesCopied = 0;

            datasetCountToCopy = 0;
            long lastByteCountTotal = 0;

            foreach (var sourceDataset in sourceFilesByDataset)
            {
                var datasetInfo = sourceDataset.Key;

                foreach (var sourceItem in sourceDataset.Value)
                {
                    if (sourceItem.IsDirectory)
                    {
                        ComputeTotalBytesAddDirectory(datasetInfo, outputDirectory, sourceItem);
                    }
                    else
                    {
                        ComputeTotalBytesAddFileIfMissing(outputDirectory, sourceItem);
                    }

                    if (TotalBytesToCopy > lastByteCountTotal)
                    {
                        datasetCountToCopy++;
                        lastByteCountTotal = TotalBytesToCopy;
                    }
                }
            }
        }

        /// <summary>
        /// Copy dataset files to the output directory
        /// </summary>
        /// <param name="sourceFilesByDataset">Source files, by dataset</param>
        /// <param name="outputDirectory">Output directory</param>
        public bool CopyDatasetFilesToTarget(
            Dictionary<DatasetInfo, List<DatasetFileOrDirectory>> sourceFilesByDataset,
            DirectoryInfo outputDirectory)
        {
            try
            {
                var debugLevel = Options.VerboseMode ? 2 : 1;

                var fileTools = new FileTools("DMSDatasetRetriever", debugLevel);
                RegisterEvents(fileTools);
                fileTools.SkipConsoleWriteIfNoProgressListener = true;

                ComputeTotalBytesToCopy(sourceFilesByDataset, outputDirectory, out var datasetCountToCopy);
                var lastProgressTime = DateTime.UtcNow;

                Console.WriteLine();

                if (datasetCountToCopy > 0)
                {
                    OnStatusEvent(
                        "Retrieving data for {0}; {1} total",
                        DMSDatasetRetriever.GetCountWithUnits(datasetCountToCopy, "dataset", "datasets"),
                        FileTools.BytesToHumanReadable(TotalBytesToCopy));

                    OnStatusEvent("Target directory: " + PathUtils.CompactPathString(outputDirectory.FullName, 200));
                    Console.WriteLine();
                }

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

                        if (TotalBytesToCopy <= 0 || Options.PreviewMode || DateTime.UtcNow.Subtract(lastProgressTime).TotalSeconds < 3)
                            continue;

                        var percentComplete = BytesCopied / (float)TotalBytesToCopy * 100;
                        OnProgressUpdate("Copying files", percentComplete);
                        lastProgressTime = DateTime.UtcNow;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CopyDatasetFilesToTarget", ex);
                return false;
            }
        }

        /// <summary>
        /// Copy remote dataset directory to the output directory
        /// </summary>
        /// <param name="fileTools">File tools</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceDirectoryInfo">Source directory</param>
        /// <param name="outputDirectory">Output directory</param>
        public void CopyDirectoryToTarget(
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
                    OnWarningEvent("Directory not found, nothing to copy: " + sourceDirectory.FullName);
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

                foreach (var subdirectory in sourceDirectory.GetDirectories())
                {
                    // RelativeTargetPath should have the target directory name, possibly preceded by a subdirectory name
                    var relativeTargetPath = Path.Combine(sourceDirectoryInfo.RelativeTargetPath, subdirectory.Name);

                    var subDirectoryInfo = new DatasetFileOrDirectory(
                        datasetInfo, subdirectory, relativeTargetPath,
                        sourceDirectoryInfo.MyEMSLDownloader);

                    CopyDirectoryToTarget(fileTools, datasetInfo, subDirectoryInfo, outputDirectory);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CopyDirectoryToTarget", ex);
            }
        }

        /// <summary>
        /// Copy remote dataset file to the output directory
        /// </summary>
        /// <param name="fileTools">File tools</param>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFileInfo">Source file info</param>
        /// <param name="outputDirectory">Output directory</param>
        public void CopyFileToTarget(
            FileTools fileTools,
            DatasetInfo datasetInfo,
            DatasetFileOrDirectory sourceFileInfo,
            FileSystemInfo outputDirectory)
        {
            try
            {
                var sourceFile = new FileInfo(sourceFileInfo.SourcePath);

                string linkFileSuffix;

                if (Options.UseDatasetLinkFiles)
                    linkFileSuffix = LINK_FILE_SUFFIX;
                else
                    linkFileSuffix = string.Empty;

                // RelativeTargetPath should have the target file name, possibly preceded by a subdirectory name
                var targetFilePath = Path.Combine(outputDirectory.FullName, sourceFileInfo.RelativeTargetPath + linkFileSuffix);

                var targetFile = new FileInfo(targetFilePath);

                if (!sourceFile.Exists)
                {
                    OnWarningEvent("File not found, nothing to copy: " + sourceFile.FullName);
                    return;
                }

                if (targetFile.Exists)
                {
                    if (Options.UseDatasetLinkFiles)
                    {
                        OnDebugEvent("Existing link file found: " + FileTools.CompactPathString(targetFile.FullName, 100));
                        datasetInfo.TargetDirectoryFiles.Add(targetFile);
                        return;
                    }

                    if (sourceFile.Length == targetFile.Length &&
                        FileTools.NearlyEqualFileTimes(sourceFile.LastWriteTime, targetFile.LastWriteTime))
                    {
                        OnDebugEvent("Skipping existing, identical file: " + FileTools.CompactPathString(targetFile.FullName, 100));
                        datasetInfo.TargetDirectoryFiles.Add(targetFile);
                        return;
                    }
                }

                if (Options.PreviewMode)
                {
                    if (Options.UseDatasetLinkFiles)
                    {
                        OnStatusEvent(
                            "Preview create link file for {0}\n  at {1}",
                            FileTools.CompactPathString(sourceFile.FullName, 100),
                            FileTools.CompactPathString(targetFile.FullName, 120));
                    }
                    else
                    {
                        OnStatusEvent(
                            "Preview copy {0}\n  to {1}",
                            FileTools.CompactPathString(sourceFile.FullName, 100),
                            FileTools.CompactPathString(targetFile.FullName, 120));
                    }
                }
                else
                {
                    CopyFileToTarget(fileTools, sourceFile, targetFile);
                }

                targetFile.Refresh();
                datasetInfo.TargetDirectoryFiles.Add(targetFile);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CopyFileToTarget", ex);
            }
        }

        private void CopyFileToTarget(FileTools fileTools, FileInfo sourceFile, FileInfo targetFile)
        {
            Console.WriteLine();

            if (Options.UseDatasetLinkFiles)
            {
                CreateLinkFile(sourceFile, targetFile);
                BytesCopied += sourceFile.Length;
            }
            else
            {
                OnStatusEvent("Retrieving " + PathUtils.CompactPathString(sourceFile.FullName, 100));

                var copySuccess = fileTools.CopyFileUsingLocks(sourceFile, targetFile.FullName, true);

                if (copySuccess)
                {
                    BytesCopied += sourceFile.Length;
                }
                else
                {
                    OnDebugEvent(
                        "Error copying {0} to {1}",
                        sourceFile.FullName,
                        PathUtils.CompactPathString(targetFile.FullName, 100));
                }
            }
        }

        private void CreateLinkFile(FileSystemInfo sourceFile, FileInfo targetFile)
        {
            try
            {
                if (targetFile.Directory?.Exists == false)
                {
                    OnStatusEvent("Creating missing directory: " + PathUtils.CompactPathString(targetFile.Directory.FullName, 100));
                    targetFile.Directory.Create();
                }

                OnStatusEvent("Creating link file: " + PathUtils.CompactPathString(targetFile.FullName, 100));

                using var writer = new StreamWriter(new FileStream(targetFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                writer.WriteLine(sourceFile.FullName);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CreateLinkFile", ex);
            }
        }
    }
}
