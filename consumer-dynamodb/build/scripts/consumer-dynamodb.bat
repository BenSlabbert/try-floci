@rem
@rem Copyright 2015 the original author or authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem
@rem SPDX-License-Identifier: Apache-2.0
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  consumer-dynamodb startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables, and ensure extensions are enabled
setlocal EnableExtensions

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
@rem This is normally unused
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and CONSUMER_DYNAMODB_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

"%COMSPEC%" /c exit 1

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

"%COMSPEC%" /c exit 1

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\consumer-dynamodb-1.0.0-SNAPSHOT.jar;%APP_HOME%\lib\model-1.0.0-SNAPSHOT.jar;%APP_HOME%\lib\vertx-core-5.0.0.CR7.jar;%APP_HOME%\lib\logback-classic-1.5.33.jar;%APP_HOME%\lib\url-connection-client-2.34.0.jar;%APP_HOME%\lib\dynamodb-2.34.0.jar;%APP_HOME%\lib\kinesis-2.34.0.jar;%APP_HOME%\lib\aws-cbor-protocol-2.34.0.jar;%APP_HOME%\lib\aws-json-protocol-2.34.0.jar;%APP_HOME%\lib\protocol-core-2.34.0.jar;%APP_HOME%\lib\aws-core-2.34.0.jar;%APP_HOME%\lib\auth-2.34.0.jar;%APP_HOME%\lib\regions-2.34.0.jar;%APP_HOME%\lib\sdk-core-2.34.0.jar;%APP_HOME%\lib\http-auth-aws-2.34.0.jar;%APP_HOME%\lib\http-auth-2.34.0.jar;%APP_HOME%\lib\http-auth-spi-2.34.0.jar;%APP_HOME%\lib\apache-client-2.34.0.jar;%APP_HOME%\lib\netty-nio-client-2.34.0.jar;%APP_HOME%\lib\http-client-spi-2.34.0.jar;%APP_HOME%\lib\profiles-2.34.0.jar;%APP_HOME%\lib\retries-2.34.0.jar;%APP_HOME%\lib\retries-spi-2.34.0.jar;%APP_HOME%\lib\identity-spi-2.34.0.jar;%APP_HOME%\lib\metrics-spi-2.34.0.jar;%APP_HOME%\lib\json-utils-2.34.0.jar;%APP_HOME%\lib\checksums-2.34.0.jar;%APP_HOME%\lib\utils-2.34.0.jar;%APP_HOME%\lib\slf4j-api-2.0.18.jar;%APP_HOME%\lib\jackson-core-2.21.3.jar;%APP_HOME%\lib\jackson-datatype-jsr310-2.21.3.jar;%APP_HOME%\lib\jackson-databind-2.21.3.jar;%APP_HOME%\lib\vertx-core-logging-5.0.0.CR7.jar;%APP_HOME%\lib\netty-handler-proxy-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-http2-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-http-4.2.0.Final.jar;%APP_HOME%\lib\netty-resolver-dns-4.2.0.Final.jar;%APP_HOME%\lib\netty-handler-4.2.0.Final.jar;%APP_HOME%\lib\netty-transport-classes-epoll-4.1.126.Final.jar;%APP_HOME%\lib\netty-transport-native-unix-common-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-socks-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-dns-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-compression-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-protobuf-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-marshalling-4.2.0.Final.jar;%APP_HOME%\lib\netty-codec-base-4.2.0.Final.jar;%APP_HOME%\lib\netty-transport-4.2.0.Final.jar;%APP_HOME%\lib\netty-buffer-4.2.0.Final.jar;%APP_HOME%\lib\netty-resolver-4.2.0.Final.jar;%APP_HOME%\lib\netty-common-4.2.0.Final.jar;%APP_HOME%\lib\logback-core-1.5.33.jar;%APP_HOME%\lib\endpoints-spi-2.34.0.jar;%APP_HOME%\lib\checksums-spi-2.34.0.jar;%APP_HOME%\lib\http-auth-aws-eventstream-2.34.0.jar;%APP_HOME%\lib\utils-lite-2.34.0.jar;%APP_HOME%\lib\annotations-2.34.0.jar;%APP_HOME%\lib\third-party-jackson-dataformat-cbor-2.34.0.jar;%APP_HOME%\lib\third-party-jackson-core-2.34.0.jar;%APP_HOME%\lib\jackson-annotations-2.21.jar;%APP_HOME%\lib\reactive-streams-1.0.4.jar;%APP_HOME%\lib\eventstream-1.0.1.jar;%APP_HOME%\lib\httpclient-4.5.13.jar;%APP_HOME%\lib\httpcore-4.4.16.jar;%APP_HOME%\lib\commons-codec-1.17.1.jar;%APP_HOME%\lib\commons-logging-1.2.jar


@rem Execute consumer-dynamodb
@rem endlocal doesn't take effect until after the line is parsed and variables are expanded
@rem which allows us to clear the local environment before executing the java command
endlocal & "%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %CONSUMER_DYNAMODB_OPTS%  -classpath "%CLASSPATH%" com.example.consumer.dynamodb.Main %* & call :exitWithErrorLevel

:exitWithErrorLevel
@rem Use "%COMSPEC%" /c exit to allow operators to work properly in scripts
"%COMSPEC%" /c exit %ERRORLEVEL%
