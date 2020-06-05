
namespace DMSDatasetRetriever
{
    class DatasetInfo
    {

        public string DatasetName { get; }

        public string ChecksumMD5 { get; set; }

        public string ChecksumSHA1 { get; set; }

        public string DatasetArchivePath { get; set; }

        public string DatasetDirectoryPath { get; set; }

        public int DatasetID { get; set; }

        public bool DatasetInMyEMSL { get; set; }

        public bool InstrumentDataPurged { get; set; }

        public string TargetDatasetName { get; set; }

        public string TargetDirectory { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName"></param>
        public DatasetInfo(string datasetName)
        {
            Clear();
            DatasetName = datasetName;
        }

        /// <summary>
        /// Reset all properties except dataset name to empty strings
        /// </summary>
        public void Clear()
        {
            ChecksumMD5 = string.Empty;
            ChecksumSHA1 = string.Empty;
            DatasetArchivePath = string.Empty;
            DatasetDirectoryPath = string.Empty;
            DatasetID = 0;
            DatasetInMyEMSL = false;
            InstrumentDataPurged = false;
            TargetDatasetName = string.Empty;
            TargetDirectory = string.Empty;

        }

        public override string ToString()
        {
            return string.Format("Dataset ID {0}: {1}", DatasetID, DatasetName);
        }

    }
}
