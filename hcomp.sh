#!/bin/bash
# Compile a hadoop program.
# Author: Cristian Di Pietrantonio
output="program.jar"
if [ $# -eq 2 ]; then
   output="$2"
fi
hadoop com.sun.tools.javac.Main $1
jar cf $output *.class
rm *.class
