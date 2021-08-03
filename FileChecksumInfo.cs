using System;
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
        /// Full (absolute) file path
        /// </summary>
        /// <remarks>Will be an empty string for data read from an existing checksum file</remarks>
        public string FullFilePath { get; set; }

        /// <summary>
        /// Relative file path
        /// </summary>
        public string RelativeFilePath { get; }

        /// <summary>
        /// Fraction number
        /// </summary>
        /// <remarks>Only present in MoTrPAC manifest files</remarks>
        [Obsolete("No longer used")]
        public int Fraction { get; set; }

        /// <summary>
        /// True if this is a technical replicate
        /// </summary>
        /// <remarks>Only present in MoTrPAC manifest files</remarks>
        [Obsolete("No longer used")]
        public bool IsTechnicalReplicate { get; set; }

        /// <summary>
        /// File comment
        /// </summary>
        /// <remarks>Only present in MoTrPAC manifest files</remarks>
        [Obsolete("No longer used")]
        public string Comment { get; set; } = string.Empty;

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
        /// <param name="relativeFilePath">Relative file path</param>
        /// <param name="fullFilePath">Full (absolute) file path; may be an empty string</param>
        public FileChecksumInfo(string relativeFilePath, string fullFilePath)
        {
            FileName = Path.GetFileName(relativeFilePath);
            RelativeFilePath = relativeFilePath;
            FullFilePath = fullFilePath;
            MD5 = string.Empty;
            MD5_Base64 = string.Empty;
            SHA1 = string.Empty;
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
