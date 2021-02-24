
# DMS Dataset Retriever

This program copies DMS instrument data files to a local computer, organizing the files into subdirectories.
The input file is a tab-delimited text file with dataset names and optionally a target subdirectory for each dataset.
The input file can also have a column for a new name to use for the dataset, supporting renaming files
to conform to a naming schema different than the original name used in DMS.

The program will also create a file listing the new dataset name along with checksums (MD5 and/or SHA-1 sum).
The MD5 checksum is computed on the fly, while the SHA-1 checksum is retrieved from DMS (if defined).

## Input File

The tab-delimited input file must have a column named Dataset. The following table shows the additional supported columns:

| Dataset          | TargetName       | TargetDirectory                    |
|------------------|------------------|------------------------------------|
| DMS Dataset Name | New dataset name | Local subdirectory for the dataset |

* Synonyms for the `Dataset` column are `DatasetName` and `Dataset Name`
* Synonyms for the `TargetName` column are `Target Name`, `New Name` and `DCC_File_Name`
  * If the TargetName column is not included, the original dataset name will be used
* Synonyms for the `TargetDirectory` column  are `Target Directory` and `DCC_Folder_Name`
  * If the TargetDirectory column is not included, the dataset will not be placed in a subdirectory of the output directory
* Other columns in the input file will be ignored

## Console Switches

Syntax:

```
DMSDatasetRetriever.exe
  /I:DatasetInfoFile [/O:OutputDirectoryPath] 
  [/ChecksumMode:Mode] [/Overwrite] 
  [/ConnectionString:DMSConnectionString] 
  [/ParentDepth:ParentDirectoryDepth]
  [/RemoteURL:RemoteUploadBaseURL]
  [/BatchFilePath:RemoteUploadBatchFile]
  [/UseLinkFiles] 
  [/Preview] [/Verbose]
```

Use `/I` to specify the tab-delimited input file with dataset names
* You can also specify the file name without `/I`

Use `/O` to specify the path to the directory where files should be copied to
* If `/I` was not used, you can also specify the output directory like this:
  * `DMSDatasetRetriever.exe DatasetInfoFile.txt G:\Upload`
  
Use `/ChecksumMode` to specify the type of checksum file to create
* `/ChecksumMode:None` disables creation of checksum files
* `/ChecksumMode:CPTAC` will create a file named Directory.cksum in the directory above each target directory
  * The file will contain SHA1-sum, then a tab, then the dataset filename (preceded by an asterisk), mirroring the output from the GNU sha1sum utility (which is included with Git For Windows)
* `/ChecksumMode:MoTrPAC` will create a file named DirectoryName_MANIFEST.txt in each target directory
  * This is a tab-delimited file with columns: raw_file, fraction, technical_replicate, tech_rep_comment, md5, sha1

By default, if the dataset file already exists locally, it will only be replaced if the size differs.
* Use `/Overwrite` to force existing files to be replaced

Use `/ConnectionString` to define the DMS database connection string
* Defaults to `/ConnectionString:Server=gigasax;Database=DMS5;Trusted_Connection=yes`

Use `/ParentDepth` to define the number of directories to traverse up from the output directory to find additional text files to add to a checksum file
* Defaults to `/ParentDepth:2`

Use `/RemoteURL` to specify the remote URL to use when creating an upload batch file
* Only applicable for `/ChecksumMode:MoTrPAC` 
* Defaults to `/RemoteURL:gs://motrpac-portal-transfer-pnnl/PASS1B-06/T70/`

Use `/BatchFilePath` to specify either the path to the directory in which to create the upload batch file or the name (or full path) of the batch file to create 
* Only applicable for `/ChecksumMode:MoTrPAC` 
* By default, the program creates a batch file named UploadFiles_yyyy-MM-dd.bat in the output directory
* Use `/BatchFilePath:F:\Upload\Test` to create the default-named batch file in the `F:\Upload\Test` directory
* Use `/BatchFilePath:UploadFiles.bat` to create the batch file in the output directory, but name it `UploadFiles.bat`
* Use `/BatchFilePath:C:\Temp\UploadFilesDCC.bat` to create the batch file at `C:\Temp\UploadFilesDCC.bat`

Use `/UseLinkFiles` to create a local placeholder text file for each remote dataset file
* This saves time and disk space by not copying the file locally, but checksum speeds will be slower (due to reading data over the network)
* Also, when the upload occurs, the data will have to be read from the storage server, then pushed to the remote server, leading to more network traffic
* Each placeholder file will have the new name for the dataset file, as specified by the `TargetName` column in the dataset info file
  * Placeholder files have the extension `.dslink`

Use `/Preview` to simulate retrieving files (or creating .dslink files)

Use `/Verbose` to see additional progress messages at the console

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov\
Website: https://panomics.pnnl.gov/ or https://omics.pnl.gov

## License

The DMS Dataset Retriever is licensed under the 2-Clause BSD License; 
you may not use this program except in compliance with the License.  You may obtain 
a copy of the License at https://opensource.org/licenses/BSD-2-Clause

Copyright 2020 Battelle Memorial Institute
