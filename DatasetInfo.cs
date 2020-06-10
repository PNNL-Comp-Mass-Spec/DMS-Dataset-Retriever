
using System.Collections.Generic;
using System.IO;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// This class tracks dataset metadata
    /// </summary>
    public class DatasetInfo
    {
        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; }

        /// <summary>
        /// Dataset storage path on the proto-x server
        /// </summary>
        public string DatasetDirectoryPath { get; set; }

        /// <summary>
        /// Dataset storage path in the archive
        /// </summary>
        /// <remarks>All datasets have this path defined, but the path will not exist if the dataset is in MyEMSL</remarks>
        public string DatasetArchivePath { get; set; }

        /// <summary>
        /// Dataset ID
        /// </summary>
        public int DatasetID { get; set; }

        /// <summary>
        /// Instrument class name
        /// </summary>
        public string InstrumentClassName { get; set; }

        /// <summary>
        /// True if the dataset is in MyEMSL
        /// </summary>
        public bool DatasetInMyEMSL { get; set; }

        /// <summary>
        /// True if the instrument data file has been purged (deleted)
        /// When true, retrieve the dataset file from MyEMSL or from DatasetArchivePath
        /// </summary>
        public bool InstrumentDataPurged { get; set; }

        /// <summary>
        /// New dataset name to use when copying to the target directory
        /// </summary>
        public string TargetDatasetName { get; set; }

        /// <summary>
        /// Target directory to which the dataset should be copied
        /// </summary>
        /// <remarks>
        /// Typically a directory on the local machine
        /// </remarks>
        public string TargetDirectory { get; set; }

        /// <summary>
        /// List of files copied to the target directory (or already present in the target directory)
        /// </summary>
        /// <remarks>This list is used to generate the checksum file</remarks>
        public List<FileInfo> TargetDirectoryFiles { get; }

        /// <summary>
        /// Dataset file name or relative path
        /// This is the file created by the instrument
        /// </summary>
        /// <remarks>
        /// Nominally comes from V_Dataset_Files_List_Report, but will be auto-defined for older datasets
        /// </remarks>
        public string DatasetFileName { get; set; }

        /// <summary>
        /// SHA-1 hash of the dataset file
        /// </summary>
        /// <remarks>
        /// Nominally comes from V_Dataset_Files_List_Report, but will be computed by this program for older datasets
        /// </remarks>
        public string DatasetFileHashSHA1 { get; set; }

        /// <summary>
        /// MD5 hash of the dataset file
        /// </summary>
        /// <remarks>
        /// If the ChecksumMode is MoTrPAC, this program will compute the MD5 hash of the dataset file
        /// </remarks>
        public string DatasetFileHashMD5 { get; set; }

        /// <summary>
        /// Dataset file size, in bytes
        /// </summary>
        public double DatasetFileSizeBytes { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public DatasetInfo(string datasetName)
        {
            DatasetName = datasetName;
            TargetDirectoryFiles = new List<FileInfo>();

            Clear();
        }

        /// <summary>
        /// Reset all properties except dataset name to empty strings
        /// Also clears TargetDirectoryFiles
        /// </summary>
        public void Clear()
        {
            DatasetDirectoryPath = string.Empty;
            DatasetArchivePath = string.Empty;

            DatasetID = 0;
            InstrumentClassName = string.Empty;

            DatasetInMyEMSL = false;
            InstrumentDataPurged = false;

            TargetDatasetName = string.Empty;
            TargetDirectory = string.Empty;
            TargetDirectoryFiles.Clear();

            DatasetFileName = string.Empty;
            DatasetFileHashSHA1 = string.Empty;
            DatasetFileHashMD5 = string.Empty;
            DatasetFileSizeBytes = 0;
        }

        /// <summary>
        /// Show the dataset ID and name
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("Dataset ID {0}: {1}", DatasetID, DatasetName);
        }

    }
}
