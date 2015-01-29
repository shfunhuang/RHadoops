rm(list=ls())
#--- page.81
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

library(rJava)
library(rhdfs)
library(rmr2)

setwd("~/R")
hdfs.init()

# MapReduce program is defined for identifying the frequency of all the words that are present 
# in the provided input text files.

# Map phase: 
# This map function will read the text file line by line and split them by spaces. 
# This map phase will assign 1 as a value to all the words that are caught by the mapper.
wc.map = function(., lines) {
  keyval(unlist(strsplit(x = lines, split = pattern)), 1)
}

# Reduce phase: 
# Reduce phase will calculate the total frequency of all the words by
# performing sum operations over words with the same keys.
wc.reduce = function(word, counts ) {
  keyval(word, sum(counts))
}

# Defining the MapReduce job: 
# After defining the word count mapper and reducer, 
# we need to create the driver method that starts the execution of MapReduce.
input <- file.path(hdfs.ls('./')$file[1], 'argo1.txt') 
output <- file.path(hdfs.ls('./')$file[3], 'argo_wc1') 

mapreduce(input = input,
          output = output,
          input.format = "text",
          map = wc.map,
          reduce = wc.reduce,
          combine = T)

hdfs.file.info(input)






