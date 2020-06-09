﻿namespace DMSDatasetRetriever
{
    class FileChecksumInfo
    {
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
        /// SHA-1 hash of the file
        /// </summary>
        public string SHA1 { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        public FileChecksumInfo(string fileName)
        {
            FileName = fileName;
            MD5 = string.Empty;
            SHA1 = string.Empty;

            Fraction = 0;
            IsTechnicalReplicate = false;
            Comment = string.Empty;
        }

        /// <summary>
        /// Show the filename
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return FileName;
        }
    }
}