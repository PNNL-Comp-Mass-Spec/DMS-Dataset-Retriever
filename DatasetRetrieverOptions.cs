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
        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "June 5, 2020";

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
            /// Create a file named DirectoryName_MANIFEST.txt in each target directory
            /// </summary>
            /// <remarks>
            /// Tab-delimited file with columns: raw_file, fraction, technical_replicate, tech_rep_comment, md5, sha1
            /// </remarks>
            MoTrPAC = 2
        }

        #endregion

        #region "Properties"

        [Option("DatasetInfoFile", "DatasetInfoFilePath", "DatasetFile", "InputFile", "i", "input",
            ArgPosition = 1, HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Dataset info file path")]
        public string DatasetInfoFilePath { get; set; }

        [Option("OutputDirectory", "OutputDirectoryPath", "Output", "o",
            ArgPosition = 2, HelpShowsDefault = false,
            HelpText = "Output directory path")]
        public string OutputDirectoryPath { get; set; }

        public ChecksumFileType ChecksumFileMode { get; private set; }

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
        /// When true, preview the files that would be retrieved
        /// </summary>
        [Option("Preview", HelpShowsDefault = false,
            HelpText = "Preview the files that would be retrieved")]
        public bool PreviewMode { get; set; }

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
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        /// <returns></returns>
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


            Console.WriteLine(" {0,-25} {1}", "Checksum file mode:", ChecksumFileModeName);

            Console.WriteLine();

            if (PreviewMode)
            {
                Console.WriteLine(" Previewing files that would be processed");
                Console.WriteLine();
            }

        }

        /// <summary>
        /// Validate the options
        /// </summary>
        /// <returns></returns>
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
