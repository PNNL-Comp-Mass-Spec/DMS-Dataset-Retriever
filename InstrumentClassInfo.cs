
namespace DMSDatasetRetriever
{
    /// <summary>
    /// This class tracks instrument class information
    /// </summary>
    internal class InstrumentClassInfo
    {
        // Ignore Spelling: tof, baf, uimf, purgable

        /// <summary>
        /// Raw data types
        /// </summary>
        public enum RawDataTypes
        {
            Unknown = 0,
            DotRawFile = 1,
            DotDFolder = 2,
            BrukerFt = 3,
            BrukerTofBaf = 4,
            DotUimfFile = 5,
            DotRawFolder = 6,
            DataFolder = 10
        }

        #region "Properties"

        /// <summary>
        /// Instrument class name
        /// </summary>
        public string InstrumentClassName { get; }

        /// <summary>
        /// True if instrument data can be purged from storage servers for instruments with this class
        /// </summary>
        public bool IsPurgable { get; }

        /// <summary>
        /// Data Type for the primary instrument file for this class
        /// </summary>
        public RawDataTypes RawDataType { get; }

        /// <summary>
        /// Instrument class comment
        /// </summary>
        public string Comment { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="instrumentClassName">Instrument class name</param>
        /// <param name="rawDataType">Raw data type</param>
        /// <param name="isPurgable">True if is purgable</param>
        /// <param name="comment">Comment</param>
        public InstrumentClassInfo(string instrumentClassName, string rawDataType, bool isPurgable, string comment)
        {
            InstrumentClassName = instrumentClassName;
            RawDataType = GetRawDataTypeByName(rawDataType);
            IsPurgable = isPurgable;
            Comment = comment;
        }

        /// <summary>
        /// Convert from raw data type name to the enum
        /// </summary>
        /// <param name="rawDataType">Raw data type</param>
        private RawDataTypes GetRawDataTypeByName(string rawDataType)
        {
            switch (rawDataType.ToLower())
            {
                case "bruker_ft":
                    return RawDataTypes.BrukerFt;

                case "bruker_tof_baf":
                    return RawDataTypes.BrukerTofBaf;

                case "data_folders":
                    return RawDataTypes.DataFolder;

                case "dot_d_folders":
                    return RawDataTypes.DotDFolder;

                case "dot_raw_files":
                    return RawDataTypes.DotRawFile;

                case "dot_uimf_files":
                    return RawDataTypes.DotUimfFile;

                case "dot_raw_folder":
                    return RawDataTypes.DotRawFolder;

                default:
                    return RawDataTypes.Unknown;
            }
        }

        /// <summary>
        /// Show the instrument class name and raw data type
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0}: {1}", InstrumentClassName, RawDataType.ToString());
        }
    }
}
