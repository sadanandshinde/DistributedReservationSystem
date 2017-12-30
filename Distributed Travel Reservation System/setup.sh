#!/bin/bash

rmiregistry -J-Djava.rmi.server.useCodebaseOnly=false 1099 &

export CLASSPATH=/home/2015/nakdag/comp512/ProjectPhase3new/
cd /home/2015/nakdag/comp512/ProjectPhase3new/

find . -name "*.java" > sources.txt && javac @sources.txt

sh clearLogs.sh