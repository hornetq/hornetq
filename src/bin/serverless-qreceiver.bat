@echo off
REM This script expects to find either JAVA_HOME set or java in PATH
REM You can set JAVA_HOME here.

SET MAIN_CLASS=org.jboss.jms.serverless.client.CommonInterfaceQueueReceiver

IF "%1" == "-help" GOTO help

IF "%JAVA_HOME%" == "" (
	SET JAVA="java" 
) ELSE (
	SET JAVA="%JAVA_HOME%\bin\java"
)

SET CP=..\etc;..\lib\jboss-JMS.jar;..\lib\jboss-j2ee.jar;..\lib\jboss-common.jar;..\lib\log4j.jar;..\lib\commons-logging.jar;..\lib\jgroups.jar

%JAVA% -classpath %CP% %MAIN_CLASS% %1 %2 %3 %4 %5 %6 %7
GOTO success

:help

ECHO Usage: %0 [-help] [arguments]
EXIT 1

:success

