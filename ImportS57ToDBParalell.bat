@ ECHO OFF
ECHO.
ECHO **********Starting Paralell Process %time%
ECHO.

START cmd.exe /k ImportS57ToDB.bat US1 > US1.txt 
START cmd.exe /k ImportS57ToDB.bat US2 > US2.txt 
START cmd.exe /k ImportS57ToDB.bat US3 > US3.txt 
START cmd.exe /k ImportS57ToDB.bat US4 > US4.txt 
START cmd.exe /k ImportS57ToDB.bat US5 > US5.txt 
START cmd.exe /k ImportS57ToDB.bat US6 > US6.txt


ECHO.
ECHO **********Process Finished at %time%
ECHO.
