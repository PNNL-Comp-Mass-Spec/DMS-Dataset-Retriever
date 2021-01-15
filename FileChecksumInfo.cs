using System.IO;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// This class tracks checksum file information
    /// </summary>
    internal class FileChecksumInfo
    {
        #region "Properties"

        /// <summary>
        /// File name
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// Fraction number
        /// </summary>
        /// <remarks>Only present in MoTrPAC manifest files</remarks>
        public int Fraction { get; set; }

        /// <summary>
        /// True if this is a technical replicate
        /// </summary>
        /// <remarks>Only present in MoTrPAC manifest files</remarks>
        public bool IsTechnicalReplicate { get; set; }

        /// <summary>
        /// File comment
        /// </summary>
        /// <remarks>Only present in MoTrPAC manifest files</remarks>
        public string Comment { get; set; }

        /// <summary>
        /// MD5 hash of the file
        /// </summary>
        public string MD5 { get; set; }

        /// <summary>
        /// MD5 hash of the file, Base64 encoded
        /// </summary>
        public string MD5_Base64 { get; set; }

        /// <summary>
        /// SHA-1 hash of the file
        /// </summary>
        public string SHA1 { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName">Filename</param>
        public FileChecksumInfo(string fileName)
        {
            FileName = fileName;
            MD5 = string.Empty;
            MD5_Base64 = string.Empty;
            SHA1 = string.Empty;

            Fraction = 0;
            IsTechnicalReplicate = false;
            Comment = string.Empty;
        }

        /// <summary>
        /// Show the filename
        /// </summary>
        public override string ToString()
        {
            return FileName;
        }
    }
}
