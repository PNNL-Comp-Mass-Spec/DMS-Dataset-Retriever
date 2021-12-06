using System;
using System.IO;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// This class tracks information on a dataset file or directory
    /// </summary>
    internal class DatasetFileOrDirectory
    {
        // Ignore Spelling: Downloader

        /// <summary>
        /// Dataset info
        /// </summary>
        public DatasetInfo DatasetInfo { get; }

        /// <summary>
        /// True if the source item is a directory instead of a file
        /// </summary>
        public bool IsDirectory { get; }

        /// <summary>
        /// Full path to the source file (or directory)
        /// </summary>
        public string SourcePath { get; }

        /// <summary>
        /// Relative target file (or directory) path
        /// </summary>
        /// <remarks>Use this when a file (or directory) residues in a subdirectory below the dataset directory</remarks>
        public string RelativeTargetPath { get; }

        /// <summary>
        /// MyEMSL file downloader
        /// </summary>
        public MyEMSLReader.Downloader MyEMSLDownloader { get; }

        /// <summary>
        /// True if the file needs to be retrieved from MyEMSL
        /// </summary>
        public bool RetrieveFromMyEMSL { get; }

        /// <summary>
        /// Constructor for copying a file
        /// </summary>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="relativeTargetFilePath">Relative target file path</param>
        /// <param name="downloader">MyEMSL Downloader</param>
        public DatasetFileOrDirectory(DatasetInfo datasetInfo, string sourceFilePath, string relativeTargetFilePath, MyEMSLReader.Downloader downloader = null)
        {
            DatasetInfo = datasetInfo;
            SourcePath = sourceFilePath;
            RelativeTargetPath = relativeTargetFilePath;

            IsDirectory = false;

            MyEMSLDownloader = downloader;
            RetrieveFromMyEMSL = (downloader != null);
        }

        /// <summary>
        /// Constructor for copying a file or a directory
        /// </summary>
        /// <param name="datasetInfo">Dataset info</param>
        /// <param name="sourceFileOrDirectory">Source file or directory</param>
        /// <param name="relativeTargetPath">Relative target path</param>
        /// <param name="downloader">MyEMSL Downloader</param>
        public DatasetFileOrDirectory(
            DatasetInfo datasetInfo,
            FileSystemInfo sourceFileOrDirectory,
            string relativeTargetPath,
            MyEMSLReader.Downloader downloader = null)
        {
            DatasetInfo = datasetInfo;

            if (sourceFileOrDirectory is FileInfo sourceFile)
            {
                SourcePath = sourceFile.FullName;
                IsDirectory = false;
            }
            else if (sourceFileOrDirectory is DirectoryInfo sourceDirectory)
            {
                SourcePath = sourceDirectory.FullName;
                IsDirectory = true;
            }
            else
            {
                throw new Exception("Cannot instantiate a new DatasetItemInfo; source item is not a file or directory: " + sourceFileOrDirectory);
            }

            RelativeTargetPath = relativeTargetPath;

            MyEMSLDownloader = downloader;
            RetrieveFromMyEMSL = (downloader != null);
        }

        /// <summary>
        /// Show the file or directory path
        /// </summary>
        public override string ToString()
        {
            if (IsDirectory)
                return "Directory: " + SourcePath;

            return "File: " + SourcePath;
        }
    }
}
