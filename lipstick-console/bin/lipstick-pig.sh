#!/usr/bin/env bash

# This is a lightly modified version of the pig run script that worked for at least one user.
# You must set the LIPSTICK_PIG_JAR environment variable to point to your lipstick-*-withHadoop jar, 
# and LIPSTICK_SERVER_URL to give the address and port of the lipstick UI server.

LIPSTICK_PIG_JAR=${LIPSTICK_PIG_JAR-/usr/local/lipstick/lipstick-console/build/libs/lipstick-console-0.7-SNAPSHOT-withHadoop.jar}
LIPSTICK_SERVER_URL=${$LIPSTICK_SERVER_URL-http://localhost:9292}

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 
# The Pig command script
#
# Environment Variables
#
#     JAVA_HOME                The java implementation to use.    Overrides JAVA_HOME.
#
#     PIG_CLASSPATH Extra Java CLASSPATH entries.
#
#     PIG_USER_CLASSPATH_FIRST If set, add user provided classpath entries to
#                              the top of classpath instead of appending them.
#                              Default is unset, i.e. the classpath entries are
#                              placed normally at the end of a pre-defined classpath.
#
#     HADOOP_HOME/HADOOP_PREFIX     Environment HADOOP_HOME/HADOOP_PREFIX(0.20.205)
#
#     HADOOP_CONF_DIR     Hadoop conf dir
#
#     PIG_HEAPSIZE    The maximum amount of heap to use, in MB. 
#                                        Default is 1000.
#
#     PIG_OPTS            Extra Java runtime options.
#
#     PIG_CONF_DIR    Alternate conf dir. Default is ${PIG_HOME}/conf.
#
#     HBASE_HOME       Optionally, the HBase installation directory.
#                      Defaults to ${PIG_HOME}/share/hbase
#
#     HBASE_CONF_DIR - Optionally, the HBase configuration to run against
#                      when using HBaseStorage. Defaults to ${HBASE_HOME}/conf

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac
debug=false

remaining=()
includeHCatalog="";
addJarString=-Dpig.additional.jars\=;
additionalJars="";
# filter command line parameter
for f in "$@"; do
     if [[ $f == "-secretDebugCmd" || $f == "-printCmdDebug" ]]; then
        debug=true
      elif [[ $f == "-useHCatalog" ]]; then
        # if need to use hcatalog, we need to add the hcatalog and hive jars
        # to the classpath and also include the hive configuration xml file
        # for pig to work correctly with hcatalog
        # because of PIG-2532, including the jars in the classpath is 
        # sufficient to ensure that they are registered as well
        includeHCatalog=true;
      elif [[ "$includeHCatalog" == "true" && $f == $addJarString* ]]; then
        additionalJars=`echo $f | sed s/$addJarString//`
      else
        remaining[${#remaining[@]}]="$f"
     fi
done

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"

# convert relative path to absolute path
bin=$(cd -P -- "$(dirname -- "$this")">/dev/null && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

# the root of the Pig installation
if [ -z "$PIG_HOME" ]; then
    export PIG_HOME=`dirname "$this"`/..
fi

if [ -z "$PIG_CONF_DIR" ]; then
    if [ -f ${PIG_HOME}/conf/pig.properties ]; then
        PIG_CONF_DIR=${PIG_HOME}/conf
    fi
fi

if [ -z "$PIG_CONF_DIR" ]; then
    if [ -d /etc/pig ]; then
        # if installed with rpm/deb package
        PIG_CONF_DIR="/etc/pig"
    fi
fi

if [ -f "${PIG_CONF_DIR}/pig-env.sh" ]; then
    . "${PIG_CONF_DIR}/pig-env.sh"
fi

# some Java parameters
if [ "$JAVA_HOME" != "" ]; then
    #echo "run java in $JAVA_HOME"
    JAVA_HOME=$JAVA_HOME
fi
    
if [ "$JAVA_HOME" = "" ]; then
    echo "Error: JAVA_HOME is not set."
    exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m 

# check envvars which might override default args
if [ "$PIG_HEAPSIZE" != "" ]; then
    JAVA_HEAP_MAX="-Xmx""$PIG_HEAPSIZE""m"
fi

# CLASSPATH initially contains $PIG_CONF_DIR
CLASSPATH="${PIG_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
if [ "$includeHCatalog" == "true" ]; then
  # need to provide the hcatalog jar file path as well as
  # the location of the hive jars on which hcatalog depends
  hiveMetaStoreJar=hive-metastore-*.jar
  thriftJar=libthrift-*.jar
  hiveExecJar=hive-exec-*.jar
  fbJar=libfb303-*.jar
  jdoECJar=jdo*-api-*.jar
  slfJar=slf4j-api-*.jar
  hbaseHiveJar=hive-hbase-handler-*.jar
  if [ "$HIVE_HOME" == "" ]; then
    if [ -d "/usr/lib/hive" ]; then
      HIVE_HOME=/usr/lib/hive
    else
      echo "Please initialize HIVE_HOME"
      exit -1
    fi
  fi

  hiveMetaStoreVersion=`ls $HIVE_HOME/lib/$hiveMetaStoreJar`
  thriftVersion=`ls $HIVE_HOME/lib/$thriftJar`
  hiveExecVersion=`ls $HIVE_HOME/lib/$hiveExecJar`
  fbJarVersion=`ls $HIVE_HOME/lib/$fbJar`
  jdoECJarVersion=`ls $HIVE_HOME/lib/$jdoECJar`
  slfJarVersion=`ls $HIVE_HOME/lib/$slfJar`
  hbaseHiveVersion=`ls $HIVE_HOME/lib/$hbaseHiveJar`

  # hcatalog jar name for 0.4 and earlier
  hcatJarOld=hcatalog-*.jar
  # hcatalog jar name for 0.5 and newer
  hcatJar=*hcatalog-core-*.jar
  hbaseHCatJar=*hbase-storage-handler-*.jar
  pigHCatJar=*hcatalog-pig-adapter-*.jar
  if [ "$HCAT_HOME" == "" ]; then
    if [ -d "/usr/lib/hcatalog" ]; then
      HCAT_HOME=/usr/lib/hcatalog
    elif [ -d "/usr/lib/hive-hcatalog" ]; then
      HCAT_HOME=/usr/lib/hive-hcatalog
    else
      echo "Please initialize HCAT_HOME"
      exit -1
    fi
  fi
  hcatJarPath=`ls $HCAT_HOME/share/hcatalog/$hcatJar`
  # if hcat jar is not found may be we are on hcatalog 0.4 or older
  if [ 'xx' == "x${hcatJarPath}x" ]; then
    hcatJarPath=`ls $HCAT_HOME/share/hcatalog/$hcatJarOld | grep -v server`
  fi

  # if we are using an older hcatalog version then the jar is on a different path
  if [ -d "$HCAT_HOME/share/hcatalog/storage-handlers/hbase/lib" ]; then
    # in 0.5 and newer we need to add multiple jars to the class path
    hbaseHCatJarPath="$HCAT_HOME/share/hcatalog/storage-handlers/hbase/lib/*"
  else
    hbaseHCatJarPath=`ls $HCAT_HOME/lib/$hbaseHCatJar`
  fi
  
  # get the pig storage handler jar
  pigHCatJarPath=`ls $HCAT_HOME/share/hcatalog/${pigHCatJar}`

  ADDITIONAL_CLASSPATHS=$hiveMetaStoreVersion:$thriftVersion:$hiveExecVersion:$fbJarVersion:$jdoECJarVersion:$slfJarVersion:$hbaseHiveVersion:$hcatJarPath:$hbaseHCatJarPath:$pigHCatJarPath
  if [ "$additionalJars" != "" ]; then
    ADDITIONAL_CLASSPATHS=$ADDITIONAL_CLASSPATHS:$additionalJars
  fi
  CLASSPATH=${CLASSPATH}:$ADDITIONAL_CLASSPATHS:$HIVE_HOME/conf
fi

# Add user-specified CLASSPATH entries via PIG_CLASSPATH
# If PIG_USER_CLASSPATH_FIRST is set, prepend the entries
if [ "$PIG_CLASSPATH" != "" ]; then
  if [ "$PIG_USER_CLASSPATH_FIRST" == "" ]; then
    CLASSPATH=${CLASSPATH}:${PIG_CLASSPATH}
  else
    CLASSPATH=${PIG_CLASSPATH}:${CLASSPATH}
  fi
fi

# add HADOOP_CONF_DIR
if [ "$HADOOP_CONF_DIR" != "" ]; then
    CLASSPATH=${CLASSPATH}:${HADOOP_CONF_DIR}
fi

# so that filenames w/ spaces are handled correctly in loops below
IFS=

shopt -s extglob
shopt -s nullglob

for f in $PIG_HOME/lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
done

JYTHON_JAR=`echo ${PIG_HOME}/lib/jython*.jar`

if [ -z "$JYTHON_JAR" ]; then
    JYTHON_JAR=`echo $PIG_HOME/build/ivy/lib/Pig/jython*.jar`
    if [ -n "$JYTHON_JAR" ]; then
        CLASSPATH=${CLASSPATH}:$JYTHON_JAR
    fi
fi

JRUBY_JAR=`echo ${PIG_HOME}/lib/jruby-complete-*.jar`

if [ -z "$JRUBY_JAR" ]; then
    JRUBY_JAR=`echo $PIG_HOME/build/ivy/lib/Pig/jruby-complete-*.jar`
    if [ -n "$JRUBY_JAR" ]; then
        CLASSPATH=${CLASSPATH}:$JRUBY_JAR
    fi
fi

for f in $PIG_HOME/share/pig/lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f;
done

# For Hadoop 0.23.0+
#
#if [ -d "${PIG_HOME}/share/hadoop/common" ]; then
#    for f in ${PIG_HOME}/share/hadoop/common/hadoop*.jar; do
#        CLASSPATH=${CLASSPATH}:$f;
#    done
#fi
#
#if [ -d "${PIG_HOME}/share/hadoop/hdfs" ]; then
#    for f in ${PIG_HOME}/share/hadoop/hdfs/hadoop*.jar; do
#        CLASSPATH=${CLASSPATH}:$f;
#    done
#fi
#
#if [ -d "${PIG_HOME}/share/hadoop/mapreduce" ]; then
#    for f in ${PIG_HOME}/share/hadoop/mapreduce/hadoop*.jar; do
#        CLASSPATH=${CLASSPATH}:$f;
#    done
#fi

if which hadoop >/dev/null; then
    HADOOP_BIN=`which hadoop`
fi

if [[ -z "$HADOOP_BIN" && -n "$HADOOP_PREFIX" ]]; then
    if [ -f $HADOOP_PREFIX/bin/hadoop ]; then
        HADOOP_BIN=$HADOOP_PREFIX/bin/hadoop
    fi
fi

if [[ -z "$HADOOP_BIN" && -n "$HADOOP_HOME" && -d "$HADOOP_HOME" ]]; then
    if [ -f $HADOOP_HOME/bin/hadoop ]; then
        HADOOP_BIN=$HADOOP_HOME/bin/hadoop
    fi
fi

if [ -z "$HADOOP_BIN" ]; then
    # if installed with rpm/deb package
    if [ -f /usr/bin/hadoop ]; then
        HADOOP_BIN=/usr/bin/hadoop
    fi
fi

# find out the HADOOP_HOME in order to find hadoop jar
# we use the name of hadoop jar to decide if user is using
# hadoop 1 or hadoop 2
if [[ -z "$HADOOP_HOME" && -n "$HADOOP_PREFIX" ]]; then
    HADOOP_HOME=$HADOOP_PREFIX
fi

if [[ -z "$HADOOP_HOME" && -n "$HADOOP_BIN" ]]; then
    HADOOP_HOME=`dirname $HADOOP_BIN`/..
fi

HADOOP_CORE_JAR=`echo ${HADOOP_HOME}/hadoop-core*.jar`

if [ -z "$HADOOP_CORE_JAR" ]; then
    HADOOP_VERSION=2
else
    HADOOP_VERSION=1
fi

# if using HBase, likely want to include HBase jars and config
HBH=${HBASE_HOME:-"${PIG_HOME}/share/hbase"}
if [ -d "${HBH}" ]; then
    for f in ${HBH}/hbase-*.jar; do
        CLASSPATH=${CLASSPATH}:$f
    done
    for f in ${HBH}/lib/*.jar; do
        CLASSPATH=${CLASSPATH}:$f
    done
    HBASE_CONF_DIR=${HBASE_CONF_DIR:-"${HBH}/conf"}
fi
if [ -n "$HBASE_CONF_DIR" ] && [ -d "$HBASE_CONF_DIR" ]; then
    CLASSPATH=$HBASE_CONF_DIR:$CLASSPATH
fi

if [ -d "${PIG_HOME}/etc/hadoop" ]; then
    CLASSPATH=${CLASSPATH}:${PIG_HOME}/etc/hadoop;
fi

# locate ZooKeeper
ZKH=${ZOOKEEPER_HOME:-"${PIG_HOME}/share/zookeeper"}
if [ -d "$ZKH" ] ; then
    for f in ${ZKH}/zookeeper-*.jar; do
        CLASSPATH=${CLASSPATH}:$f
    done
fi

# default log directory & file
if [ "$PIG_LOG_DIR" = "" ]; then
    PIG_LOG_DIR="$PIG_HOME/logs"
fi
if [ "$PIG_LOGFILE" = "" ]; then
    PIG_LOGFILE='pig.log'
fi

# cygwin path translation
if $cygwin; then
    CLASSPATH=`cygpath -p -w "$CLASSPATH"`
    PIG_HOME=`cygpath -d "$PIG_HOME"`
    PIG_LOG_DIR=`cygpath -d "$PIG_LOG_DIR"`
fi
 
# restore ordinary behaviour
unset IFS

PIG_OPTS="$PIG_OPTS -Dpig.log.dir=$PIG_LOG_DIR"
PIG_OPTS="$PIG_OPTS -Dpig.log.file=$PIG_LOGFILE"
PIG_OPTS="$PIG_OPTS -Dpig.home.dir=$PIG_HOME"
if [ "$includeHCatalog" == "true" ]; then
  addJars=`echo $PIG_OPTS | awk '{ for (i=1; i<=NF; i++) print $i; }' | grep "\-Dpig.additional.jars=" | sed s/-Dpig.additional.jars=//`
  if [ "$addJars" != "" ]; then
    ADDITIONAL_CLASSPATHS=$addJars:$ADDITIONAL_CLASSPATHS
    PIG_OPTS=`echo $PIG_OPTS | sed 's/-Dpig.additional.jars=[^ ]*//'`
  fi
  PIG_OPTS="$PIG_OPTS -Dpig.additional.jars=$ADDITIONAL_CLASSPATHS"
fi

# run it
if [ -n "$HADOOP_BIN" ]; then
    if [ "$debug" == "true" ]; then
        echo "Find hadoop at $HADOOP_BIN"
    fi

    # if [ -f $PIG_HOME/pig-withouthadoop-h${HADOOP_VERSION}.jar ]; then
    #     PIG_JAR=$PIG_HOME/pig-withouthadoop-h${HADOOP_VERSION}.jar
    # else
    #     PIG_JAR=`echo $PIG_HOME/pig-?.*withouthadoop-h${HADOOP_VERSION}.jar`
    # fi

#########
#
# Lipstick Changes
#
    PIG_JAR=$LIPSTICK_PIG_JAR
    PIG_OPS="$PIG_OPTS -Dlipstick.server.url=$LIPSTICK_SERVER_URL"
#
#
######


    # for deb/rpm package, add pig jar in /usr/share/pig
    if [ -z "$PIG_JAR" ]; then
        PIG_JAR=`echo $PIG_HOME/share/pig/pig-*withouthadoop-h${HADOOP_VERSION}.jar`
    fi

    if [ -n "$PIG_JAR" ]; then
        CLASSPATH=${CLASSPATH}:$PIG_JAR
    else
        if [ "$HADOOP_VERSION" == "1" ]; then
            echo "Cannot locate pig-withouthadoop-h${HADOOP_VERSION}.jar. do 'ant jar-withouthadoop', and try again"
        else
            echo "Cannot locate pig-withouthadoop-h${HADOOP_VERSION}.jar. do 'ant -Dhadoopversion=23 jar-withouthadoop', and try again"
        fi
        exit 1
    fi

    export HADOOP_CLASSPATH=$CLASSPATH:$HADOOP_CLASSPATH
    export HADOOP_OPTS="$JAVA_HEAP_MAX $PIG_OPTS $HADOOP_OPTS"
    export HADOOP_CLIENT_OPTS="$JAVA_HEAP_MAX $PIG_OPTS $HADOOP_CLIENT_OPTS"
    if [ "$debug" == "true" ]; then
        echo "dry run:"
        echo "HADOOP_CLASSPATH: $HADOOP_CLASSPATH"
        echo "HADOOP_OPTS: $HADOOP_OPTS"
        echo "HADOOP_CLIENT_OPTS: $HADOOP_CLIENT_OPTS"
        echo "$HADOOP_BIN" jar "$PIG_JAR" "${remaining[@]}"
        echo
    else
        exec "$HADOOP_BIN" jar "$PIG_JAR" "${remaining[@]}"
    fi
else
    # fall back to use fat pig.jar
    if [ -f $PIG_HOME/pig-h1.jar ]; then
        PIG_JAR=$PIG_HOME/pig-h1.jar
    else
        PIG_JAR=`echo $PIG_HOME/pig-?.!(*withouthadoop)-h1.jar`
    fi

    if [ -n "$PIG_JAR" ]; then
        CLASSPATH="${CLASSPATH}:$PIG_JAR"
    else
        echo "Cannot locate pig.jar. do 'ant jar', and try again"
        exit 1
    fi

    if [ "$debug" == "true" ]; then
        echo "Cannot find local hadoop installation, using bundled `java -cp $PIG_JAR org.apache.hadoop.util.VersionInfo | head -1`"
    fi

    CLASS=org.apache.pig.Main
    if [ "$debug" == "true" ]; then
        echo "dry run:"
        echo "$JAVA" $JAVA_HEAP_MAX $PIG_OPTS -classpath "$CLASSPATH" $CLASS "${remaining[@]}"
        echo
    else
        exec "$JAVA" $JAVA_HEAP_MAX $PIG_OPTS -classpath "$CLASSPATH" $CLASS "${remaining[@]}"
    fi
fi
shopt -u nullglob
shopt -u extglob
