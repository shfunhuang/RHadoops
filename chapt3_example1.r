rm(list=ls())
#--- page.79
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

library(rJava)
library(rhdfs)
library(rmr2)

setwd("~/R")
hdfs.init()

# defining input parameters as small.ints hdfs object, map parameter as
# function to calculate the min and max for generated random deviates. 
small.ints = to.dfs(1:10)
mapr = mapreduce(input = small.ints,
                 map = function(k, v){
                        lapply(seq_along(v), function(r){
                        x <- runif(v[[r]])
                        keyval(r,c(max(x),min(x)))
    })
  }
)
output <- from.dfs(mapr)
table_output <- do.call('rbind', lapply(X=output$val, FUN="[[",2))
table_output
#for(i in 1:10){
#  print(do.call("cbind", lapply(output$val[[i]]$val, rbind)))
#}

