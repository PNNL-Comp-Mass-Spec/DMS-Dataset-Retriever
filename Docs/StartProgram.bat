@echo on

del ThermoUploadTest_LinkFiles\*.dslink /q

del ThermoUploadTest_MoTrPAC_LinkFiles\*.txt /s /q
del ThermoUploadTest_MoTrPAC_LinkFiles\*.dslink /s /q

..\bin\DMSDatasetRetriever.exe /conf:ThermoUploadTest.conf
sleep 2

..\bin\DMSDatasetRetriever.exe /conf:ThermoUploadTest_LinkFiles.conf
sleep 2

..\bin\DMSDatasetRetriever.exe /conf:ThermoUploadTest_CPTAC.conf
sleep 2

..\bin\DMSDatasetRetriever.exe /conf:ThermoUploadTest_MoTrPAC.conf
sleep 2

..\bin\DMSDatasetRetriever.exe /conf:ThermoUploadTest_MoTrPAC_LinkFiles.conf

@echo off
echo.
echo About to delete local .raw files to free up disk space
pause

@echo on

del ThermoUploadTest\*.raw /s /q
del ThermoUploadTest_CPTAC\*.raw /s /q
del ThermoUploadTest_LinkFiles\*.raw /s /q
del ThermoUploadTest_MoTrPAC\*.raw /s /q
del ThermoUploadTest_MoTrPAC_LinkFiles\*.raw /s /q
