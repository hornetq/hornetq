#!/bin/sh

# Copyright 2009 Red Hat, Inc.
# Red Hat licenses this file to you under the Apache License, version
# 2.0 (the "License"); you may not use htis file except in compliance
# with the License. You may obtain a copy of the License at
# http://www.apache.org/license/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# ------------------------------------------------
# HornetQ Build Script for Linux
# ------------------------------------------------

# $Id: $

# This build script will ensure the correct ant version is used.
# Users can also pass in options on the command line.

warn() {
	echo "${PROGNAME}: $*"
}

die() {
	warn $*
	exit 1
}

# Save off the original ANT_HOME value
ORIG_ANT_HOME=$ANT_HOME
export ORIG_ANT_HOME

# Set the temporary ANT_HOME

if [ -n "$OVERRIDE_ANT_HOME" ]; then

  echo "ANT_HOME is ${OVERRIDE_ANT_HOME}"
  
  ANT_HOME=$OVERRIDE_ANT_HOME
  
  export ANT_HOME	

else

   ANT_HOME=tools/ant
   
   export ANT_HOME	

fi

# Check for JAVA_HOME. If it is not there, warn the user

if [ -n "$JAVA_HOME" ]; then

	if [ -f "$JAVA_HOME/bin/javac" ]; then
		echo "Found javac"
	else
		die "Could not find javac."
	fi
else
	warn "JAVA_HOME is not set. Build may fail."
	warn "Set JAVA_HOME to the directory of your local JDK to avoid this message."	
fi

# Call ant
chmod +x $ANT_HOME/bin/ant

echo "Using the following ant version from ${ANT_HOME}:"

$ANT_HOME/bin/ant -version

$ANT_HOME/bin/ant "$@" -Dhornetq.run_script=true


# Restore the original path
ANT_HOME=$ORIG_ANT_HOME

export ANT_HOME
