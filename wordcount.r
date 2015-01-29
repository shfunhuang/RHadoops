#rm(list=ls())
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

library(rJava)
library(rhdfs)
library(rmr2)

#setwd("~/Desktop/R")
hdfs.init()

backend.parameters = 
  list(
    hadoop = 
      list(
        D = "mapred.map.child.ulimit=2097152",
        D = "mapred.reduce.child.ulimit=2097152",
        D = "mapred.tasktracker.map.tasks.maximum=1",
        D = "mapred.tasktracker.reduce.tasks.maximum=1"))

map <- function(k,lines) {
  
  # readline by rows to list type
  words.list <- strsplit(lines, '\\s')
  
  # unlist all word to character
  words <- unlist(words.list)
  
  return( keyval(words, 1) ) 
} 

reduce <- function(word, counts) { 
  keyval(word, sum(counts)) 
} 

# hdfs.put("/home/cloudera/Desktop/R/data/argo.txt", './data')
# hdfs.mkdir('./out')
# hdfs.cat(hdfs.data)

# hdfs.root <- hdfs.ls('./')$file
# hadoop fs -cat ./data/argo1.txt
# hdfs.cat("./data/argo1.txt")
hdfs.data <- file.path(hdfs.ls('./')$file[2], 'argo.txt') 
hdfs.out <- file.path(hdfs.ls('./')$file[3], paste0("argo_wc_", substr(abs(rnorm(1)), 3,10))) 

# data from hdfs
wordcount <- function (input=hdfs.data, output=hdfs.out) { 
  mapreduce(input=input, output=output, input.format="text", map=map, reduce=reduce) 
} 
out <- wordcount(hdfs.data, hdfs.out)
from.dfs(hdfs.out)


