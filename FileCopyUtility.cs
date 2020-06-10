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

        private void ComputeTotalBytesAddDirectory(
            DatasetInfo datasetInfo,
            FileSystemInfo outputDirectory,
            DatasetFileOrDirectory sourceDirectoryInfo)
        {

            var sourceDirectory = new DirectoryInfo(sourceDirectoryInfo.SourcePath);
            if (!sourceDirectory.Exists)
                return;

            // ToDo: Validate this code

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
            FileSystemInfo outputDirectory)
        {
            TotalBytesToCopy = 0;
            BytesCopied = 0;

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
                }
            }
        }

        /// <summary>
        /// Copy dataset files to the output directory
        /// </summary>
        /// <param name="sourceFilesByDataset"></param>
        /// <param name="outputDirectory"></param>
        /// <returns></returns>
        public bool CopyDatasetFilesToTarget(
            Dictionary<DatasetInfo, List<DatasetFileOrDirectory>> sourceFilesByDataset,
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
                fileTools.SkipConsoleWriteIfNoProgressListener = true;

                ComputeTotalBytesToCopy(sourceFilesByDataset, outputDirectory);
                var lastProgressTime = DateTime.UtcNow;

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
        /// <param name="fileTools"></param>
        /// <param name="datasetInfo"></param>
        /// <param name="sourceDirectoryInfo"></param>
        /// <param name="outputDirectory"></param>
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

                // ToDo: Validate this code

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
        /// <param name="fileTools"></param>
        /// <param name="datasetInfo"></param>
        /// <param name="sourceFileInfo"></param>
        /// <param name="outputDirectory"></param>
        public void CopyFileToTarget(
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
                    OnWarningEvent("File not found, nothing to copy: " + sourceFile.FullName);
                    return;
                }

                if (targetFile.Exists)
                {
                    if (sourceFile.Length == targetFile.Length &&
                        FileTools.NearlyEqualFileTimes(sourceFile.LastWriteTime, targetFile.LastWriteTime))
                    {
                        OnDebugEvent("Skipping existing, identical file: " + FileTools.CompactPathString(targetFile.FullName, 80));
                        datasetInfo.TargetDirectoryFiles.Add(targetFile);
                        return;
                    }
                }

                if (Options.PreviewMode)
                {
                    Console.WriteLine("Copy {0} to\n  {1}", sourceFile.FullName, targetFile.FullName);
                }
                else
                {
                    Console.WriteLine();

                    var copySuccess = fileTools.CopyFileUsingLocks(sourceFile, targetFile.FullName, true);
                    if (copySuccess)
                    {
                        BytesCopied += sourceFile.Length;
                    }
                    else
                    {
                        OnDebugEvent(string.Format(
                            "Error copying {0} to {1}",
                            sourceFile.FullName,
                            PathUtils.CompactPathString(targetFile.FullName, 60)));
                    }
                }

                targetFile.Refresh();
                datasetInfo.TargetDirectoryFiles.Add(targetFile);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in CopyFileToTarget", ex);
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public FileCopyUtility(DatasetRetrieverOptions options)
        {
            Options = options;
        }
    }
}
