Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

setwd("~/Desktop/R")

library(rJava)
library(rhdfs)
library(rmr2)

#--- loading data from locate
boxLevel <- readRDS("data/boxLevel.rds")
boxLevel <- read.table("data/boxLevel.txt", header=T)
head(boxLevel)

#--- initinal rhdfs
hdfs.init()

#--- make director
hdfs.mkdir('./data')

#--- remove director
#hdfs.rm('./temp')

#--- view director
hdfs.ls('./')

#--- upload data to hdfs
hdfs.put(gsub(" ", "", paste(getwd(), "/data/boxLevel.txt")), './data')
hdfs.put(gsub(" ", "", paste(getwd(), "/data/boxLevel.rds")), './data')

#--- view data infomation
hdfs.file.info('./data/boxLevel.txt')
hdfs.file.info('./data/boxLevel.rds')

#--- write data
f <- hdfs.file("./data/iris.txt","w")
data(iris)
hdfs.write(iris, f)
hdfs.close(f)

#--- read data
f <- hdfs.file("iris.txt","r")
dfserialized <- hdfs.read(f)
df <- unserialize(dfserialized)
df
hdfs.close(f)

#--- move file
hdfs.move("iris.txt","./data/iris2.txt")

#--- rename file
hdfs.rename("./data/iris2.txt","./data/iris.txt")





