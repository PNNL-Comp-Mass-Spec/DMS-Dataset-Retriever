﻿
namespace DMSDatasetRetriever
{
    class InstrumentClassInfo
    {
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

        public string Comment { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="instrumentClassName"></param>
        /// <param name="rawDataType"></param>
        /// <param name="isPurgable"></param>
        /// <param name="comment"></param>
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
        /// <param name="rawDataType"></param>
        /// <returns></returns>
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

        public override string ToString()
        {
            return string.Format("{0}: {1}", InstrumentClassName, RawDataType.ToString());
        }
    }
}
