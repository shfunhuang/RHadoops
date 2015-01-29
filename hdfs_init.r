Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

library(rJava)
library(rhdfs)
library(rmr2)

setwd("~/R")

backend.parameters = 
  list(
    hadoop = 
      list(
        D = "mapred.map.child.ulimit=2097152",
        D = "mapred.reduce.child.ulimit=2097152",
        D = "mapred.tasktracker.map.tasks.maximum=1",
        D = "mapred.tasktracker.reduce.tasks.maximum=1"))

#test rhdfs
hdfs.init()
hdfs.ls("/")

#test rmr
small.ints = to.dfs(1:20)
mapr <- mapreduce( input = small.ints, map = function(k, v) cbind(v, v^2), backend.parameters=backend.parameters )

#hdfs.get(gsub(" ","",paste(hdfs.ls("/")$file[6], "/file17586822ce0a")), gsub(" ","",paste(getwd(), "/text")))

from.dfs(mapr)



