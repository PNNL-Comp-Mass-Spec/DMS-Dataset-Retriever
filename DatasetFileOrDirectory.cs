using System;
using System.IO;

namespace DMSDatasetRetriever
{
    class DatasetFileOrDirectory
    {
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
        public string RelativeTargetPath { get; }

        public MyEMSLReader.Downloader MyEMSLDownloader { get; }

        public bool RetrieveFromMyEMSL { get; }

        /// <summary>
        /// Constructor for copying a file
        /// </summary>
        /// <param name="dataset"></param>
        /// <param name="sourceFilePath"></param>
        /// <param name="relativeTargetFilePath"></param>
        /// <param name="downloader">MyEMSL Downloader</param>
        public DatasetFileOrDirectory(DatasetInfo dataset, string sourceFilePath, string relativeTargetFilePath, MyEMSLReader.Downloader downloader = null)
        {
            DatasetInfo = dataset;
            SourcePath = sourceFilePath;
            RelativeTargetPath = relativeTargetFilePath;

            IsDirectory = false;

            MyEMSLDownloader = downloader;
            RetrieveFromMyEMSL = (downloader != null);
        }

        /// <summary>
        /// Constructor for copying a file or a directory
        /// </summary>
        /// <param name="dataset"></param>
        /// <param name="sourceFileOrDirectory"></param>
        /// <param name="relativeTargetPath"></param>
        /// <param name="downloader">MyEMSL Downloader</param>
        public DatasetFileOrDirectory(DatasetInfo dataset, FileSystemInfo sourceFileOrDirectory, string relativeTargetPath, MyEMSLReader.Downloader downloader = null)
        {
            DatasetInfo = dataset;

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
    }
}
