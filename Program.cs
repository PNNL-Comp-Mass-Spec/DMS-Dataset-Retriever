﻿using System;
using System.IO;
using System.Reflection;
using System.Threading;
using PRISM;

namespace DMSDatasetRetriever
{
    internal static class Program
    {
        // Ignore Spelling: conf

        private static int Main(string[] args)
        {
            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = DatasetRetrieverOptions.GetAppVersion();

            var parser = new CommandLineParser<DatasetRetrieverOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program copies DMS instrument data files to a local computer, organizing the files into subdirectories. " +
                              "The input file is a tab-delimited text file with dataset names and optionally a target subdirectory for each dataset. " +
                              "The input file can also have a column for a new name to use for the dataset, supporting renaming files " +
                              "to conform to a naming schema different than the original name used in DMS. ",

                ContactInfo = "Program written by Matthew Monroe for PNNL (Richland, WA)" + Environment.NewLine +
                              "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                              "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://www.pnnl.gov/integrative-omics",

                UsageExamples = {
                    exeName + " DatasetInfoFile.txt",
                    exeName + @" DatasetInfoFile.txt G:\Upload",
                    exeName + @" DatasetInfoFile.txt G:\Upload /Preview",
                    exeName + @" DatasetInfoFile.txt G:\Upload /ChecksumMode:MoTrPAC",
                    exeName + @" /I:DatasetInfoFile.txt /O:G:\Upload"
                }
            };

            parser.AddParamFileKey("Conf");

            var result = parser.ParseArgs(args);
            var options = result.ParsedResults;

            try
            {
                if (!result.Success)
                {
                    if (parser.CreateParamFileProvided)
                    {
                        return 0;
                    }

                    // Delay for 1500 msec in case the user double-clicked this file from within Windows Explorer (or started the program via a shortcut)
                    // Error messages should have already been shown to the user
                    Thread.Sleep(1500);
                    return -1;
                }

                if (!options.ValidateArgs(out var errorMessage))
                {
                    parser.PrintHelp();

                    Console.WriteLine();
                    ConsoleMsgUtils.ShowWarning("Validation error:");
                    ConsoleMsgUtils.ShowWarning(errorMessage);

                    Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.Write($"Error parsing options for {exeName}");
                Console.WriteLine(ex.Message);
                Console.WriteLine($"See help with {exeName} --help");
                return -1;
            }

            try
            {
                var processor = new DMSDatasetRetriever(options);

                processor.DebugEvent += Processor_DebugEvent;
                processor.ErrorEvent += Processor_ErrorEvent;
                processor.StatusEvent += Processor_StatusEvent;
                processor.WarningEvent += Processor_WarningEvent;
                processor.SkipConsoleWriteIfNoProgressListener = true;

                var success = processor.RetrieveDatasetFiles(options.DatasetInfoFilePath, options.OutputDirectoryPath);

                if (success)
                {
                    return 0;
                }

                Thread.Sleep(1500);
                return -1;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                Thread.Sleep(1500);
                return -1;
            }
        }

        private static void Processor_DebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebugCustom(message, emptyLinesBeforeMessage: 0);
        }

        private static void Processor_ErrorEvent(string message, Exception ex)
        {
            ConsoleMsgUtils.ShowError(message, ex);
        }

        private static void Processor_StatusEvent(string message)
        {
            Console.WriteLine(message);
        }

        private static void Processor_WarningEvent(string message)
        {
            ConsoleMsgUtils.ShowWarning(message);
        }
    }
}
