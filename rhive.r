Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

Sys.setenv(HIVE_CMD="/usr/bin/hive")

setwd("~/R")

library("rJava")
library("RHive")

rhive.connect(host="127.0.0.1")
rhive.init()
rhive.list.tables ()

