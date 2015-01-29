Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

setwd("~/Desktop/R")

library(rJava)
library(rhdfs)
library(rmr2)

#--- upload data
small.ints <- to.dfs(keyval("small.ints", 1:10))

#--- load data
from.dfs("./tmp/file32f85e7bda54")


