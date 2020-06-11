using System;
using System.IO;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// This class tracks information on a dataset file or directory
    /// </summary>
    internal class DatasetFileOrDirectory
    {
        #region "Properties"

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

        #endregion

        /// <summary>
        /// Constructor for copying a file
        /// </summary>
        /// <param name="datasetInfo"></param>
        /// <param name="sourceFilePath"></param>
        /// <param name="relativeTargetFilePath"></param>
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
        /// <param name="datasetInfo"></param>
        /// <param name="sourceFileOrDirectory"></param>
        /// <param name="relativeTargetPath"></param>
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
        /// <returns></returns>
        public override string ToString()
        {
            if (IsDirectory)
                return "Directory: " + SourcePath;

            return "File: " + SourcePath;
        }
    }
}
