using System;
using System.IO;
using System.Reflection;
using PRISM;

namespace DMSDatasetRetriever
{
    /// <summary>
    /// Dataset retrieval options
    /// </summary>
    public class DatasetRetrieverOptions
    {
        // Ignore Spelling: motrpac, pnnl, yyyy, conf, YYYYMMDD, yyyy-MM-dd

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "February 23, 2021";

        #region "Enums"

        /// <summary>
        /// Checksum file modes
        /// </summary>
        public enum ChecksumFileType
        {
            /// <summary>
            /// Do not create a checksum file
            /// </summary>
            None = 0,

            /// <summary>
            /// Create a file named Directory.cksum in the directory above each target directory
            /// </summary>
            /// <remarks>
            /// The file will contain SHA1-sum, then a tab, then the dataset filename (preceded by an asterisk),
            /// mirroring the output from the GNU sha1sum utility (which is included with Git For Windows)
            /// </remarks>
            CPTAC = 1,

            /// <summary>
            /// Create a file named file_manifest_YYYYMMDD.csv in each target directory
            /// </summary>
            /// <remarks>
            /// CSV file with columns: file_name, md5, sha1
            /// </remarks>
            MoTrPAC = 2
        }

        #endregion

        #region "Properties"

        /// <summary>
        /// File listing datasets to retrieve
        /// </summary>
        [Option("DatasetInfoFile", "DatasetInfoFilePath", "DatasetFile", "InputFile", "i", "input",
            ArgPosition = 1, HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Dataset info file path")]
        public string DatasetInfoFilePath { get; set; }

        /// <summary>
        /// Output directory
        /// </summary>
        [Option("OutputDirectory", "OutputDirectoryPath", "Output", "o",
            ArgPosition = 2, HelpShowsDefault = false,
            HelpText = "Output directory path")]
        public string OutputDirectoryPath { get; set; }

        /// <summary>
        /// Checksum file mode enum
        /// </summary>
        public ChecksumFileType ChecksumFileMode { get; private set; }

        /// <summary>
        /// Checksum file mode name
        /// </summary>
        [Option("ChecksumMode", "Checksum", HelpShowsDefault = false,
            HelpText = "Checksum type (None, CPTAC, or MoTrPAC)")]
        public string ChecksumFileModeName
        {
            get => ChecksumFileMode.ToString();
            set
            {
                if (Enum.TryParse(value, out ChecksumFileType checksumMode))
                {
                    ChecksumFileMode = checksumMode;
                }
                else
                {
                    var enumNames = string.Join(", ", Enum.GetNames(typeof(ChecksumFileType)));

                    ConsoleMsgUtils.ShowWarning(
                        "{0} is not a valid value for ChecksumMode; options are {1}",
                        value, enumNames);
                }
            }
        }

        private DateTime mChecksumFileNameDate;

        /// <summary>
        /// Checksum file name date
        /// </summary>
        public DateTime ChecksumFileNameDate
        {
            get => mChecksumFileNameDate.Equals(DateTime.MinValue) ? DateTime.Now : mChecksumFileNameDate;
            private set => mChecksumFileNameDate = value;
        }

        /// <summary>
        /// User-defined Checksum file name date
        /// </summary>
        [Option("ChecksumFileNameDateText", "ChecksumDate", HelpShowsDefault = false,
            HelpText = "Date to use when generating the checksum filename when the Checksum Mode is MOTRPAC")]
        public string ChecksumFileNameDateText
        {
            get => ChecksumFileNameDate.ToString("yyyy-MM-dd");
            set => ChecksumFileNameDate = DateTime.TryParse(value, out var fileNameDate) ? fileNameDate : DateTime.MinValue;
        }

        /// <summary>
        /// DMS database connection string
        /// </summary>
        /// <remarks>
        /// The default, which uses Server, Database, and Trusted_Connection, is equivalent to
        /// "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI"
        /// </remarks>
        [Option("DMSConnectionString", "ConnectionString", "CN", HelpShowsDefault = false,
            HelpText = "DMS database connection string")]
        public string DMSConnectionString { get; set; } = "Server=gigasax;Database=DMS5;Trusted_Connection=yes";

        /// <summary>
        /// Number of parent directories to traverse up when finding additional text files to upload
        /// </summary>
        [Option("ParentDirectoryDepth", "ParentDepth", HelpShowsDefault = false,
            HelpText = "When creating the batch file with upload commands, " +
                       "look for additional text files\nin directories below the parent directory (if ParentDirectoryDepth=1)\n" +
                       "or below the parent of the parent directory (if ParentDirectoryDepth=2)")]
        public int ParentDirectoryDepth { get; set; } = 2;

        /// <summary>
        /// When true, preview the files that would be retrieved
        /// </summary>
        [Option("Preview", HelpShowsDefault = false,
            HelpText = "Preview the files that would be retrieved")]
        public bool PreviewMode { get; set; }

        [Option("RemoteUploadBaseURL", "RemoteUploadURL", "RemoteURL", HelpShowsDefault = false,
            HelpText = "Remote upload base URL to use when creating the batch file with upload commands (MoTrPAC only);\n" +
                       "defaults to the MoTrPAC Google cloud bucket.\n" +
                       "Local data files must be organized in a hierarchy that matches the directory names in this URL")]
        // ReSharper disable once StringLiteralTypo
        public string RemoteUploadBaseURL { get; set; } = "gs://motrpac-portal-transfer-pnnl/PASS1B-06/T70/";

        /// <summary>
        /// Remote upload batch file path (or directory for the batch file)
        /// </summary>
        [Option("RemoteUploadBatchFilePath", "RemoteUploadBatchFile", "BatchFilePath", HelpShowsDefault = false,
            HelpText = "Path to the directory in which to create the upload batch file " +
                       "(default name UploadFiles_yyyy-MM-dd_DatasetInfoFileName.bat);\n" +
                       "alternatively, the name (or full path) of the batch file to create (the name must end in '.bat')")]
        public string RemoteUploadBatchFilePath { get; set; } = string.Empty;

        /// <summary>
        /// /When true, use dataset link files
        /// </summary>
        [Option("UseDatasetLinkFiles", "UseLinkFiles", "CreateLinks", "MakeLinks", HelpShowsDefault = false,
            HelpText = "When true, for each remote dataset file, " +
                       "create a local text file that contains the remote file path.\n" +
                       "This saves time and disk space by not copying the file locally, " +
                       "but checksum speeds will be slower (due to reading data over the network).\n" +
                       "Also, when the upload occurs, the data will have to be read from the storage server, " +
                       "then pushed to the remote server, leading to more network traffic.\n" +
                       // ReSharper disable once StringLiteralTypo
                       "Link files have extension .dslink")]
        public bool UseDatasetLinkFiles { get; set; } = false;

        /// <summary>
        /// When true, show more status messages
        /// </summary>
        [Option("VerboseMode", "Verbose", "V", HelpShowsDefault = false,
            HelpText = "When true, show more status messages")]
        public bool VerboseMode { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public DatasetRetrieverOptions()
        {
            DatasetInfoFilePath = string.Empty;
            OutputDirectoryPath = string.Empty;
            ChecksumFileMode = ChecksumFileType.CPTAC;
            ChecksumFileNameDateText = string.Empty;
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        public static string GetAppVersion()
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";

            return version;
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");

            Console.WriteLine(" {0,-25} {1}", "Dataset info file:", DatasetInfoFilePath);

            if (string.IsNullOrEmpty(OutputDirectoryPath))
            {
                var currentDirectory = new DirectoryInfo(".");
                Console.WriteLine(" {0,-25} {1}", "Output directory:", currentDirectory.FullName);
            }
            else
            {
                Console.WriteLine(" {0,-25} {1}", "Output directory:", OutputDirectoryPath);
            }

            Console.WriteLine(" {0,-25} {1}", "Use dataset link files:", UseDatasetLinkFiles);

            Console.WriteLine(" {0,-25} {1}", "Checksum file mode:", ChecksumFileModeName);

            Console.WriteLine(" {0,-25} {1:yyyy-MM-dd}", "Checksum file date:", ChecksumFileNameDate);

            if (ChecksumFileMode == ChecksumFileType.MoTrPAC)
            {
                Console.WriteLine();
                Console.WriteLine("Upload batch file options:");
                Console.WriteLine(" {0,-25} {1}", "Parent directory depth:", ParentDirectoryDepth);
                Console.WriteLine(" {0,-25} {1}", "Remote Upload Base URL:", RemoteUploadBaseURL);

                if (!string.IsNullOrWhiteSpace(RemoteUploadBatchFilePath))
                {
                    if (RemoteUploadBatchFilePath.Trim().EndsWith(".bat", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine(" {0} {1}", "Remote upload batch file path:", RemoteUploadBatchFilePath);
                    }
                    else
                    {
                        Console.WriteLine(" {0} {1}", "Directory to create the remote upload batch file:", RemoteUploadBatchFilePath);
                    }
                }
            }

            Console.WriteLine();

            if (PreviewMode)
            {
                Console.WriteLine(" Previewing files that would be processed");
                Console.WriteLine();
            }

            if (DatasetInfoFilePath.EndsWith(".conf", StringComparison.OrdinalIgnoreCase))
            {
                ConsoleMsgUtils.ShowWarning(
                    "Dataset info file ends in '.conf' -- you probably meant to use {0}",
                    "/conf:" + DatasetInfoFilePath);
                Console.WriteLine();
            }
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        public bool ValidateArgs(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(DatasetInfoFilePath))
            {
                errorMessage = "You must specify the dataset info file";
                return false;
            }

            errorMessage = string.Empty;
            return true;
        }
    }
}
