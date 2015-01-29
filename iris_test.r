#rm(list=ls())
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

library(rJava)
library(rhdfs)
library(rmr2)

hdfs.init()

data(iris)
iris.values <- to.dfs(iris)
                          
#--- MAP
map <- function(k,v){
  shuffler <- function(v, p1=0.8, p2=0.2){    
    data.index <- sample(0:1, nrow(v), prob=c(p2, p1), rep=T)
    data_train <- v[data.index==1, ]
    data_test <- v[data.index==0, ] 
    list(train=data_train, test=data_test, p1=p1, p2=p2)    
  }
  s.val <- shuffler(v)
  
  val <- s.val$train
  key <- as.integer(rownames(val))
  return(keyval(key, val))
}

#--- REDUCE
reduce <- function(k, v) { 
  iris.rf <- randomForest(formula=Species~., 
                          data=v, 
                          ntree= 50,
                          proximity=TRUE,
                          importance=TRUE, 
                          do.trace=FALSE,
                          na.action=na.roughfix)
  val <- iris.rf$err.rate
  return(keyval(key, val))
} 

### test randon forest sampling with mr
mr1 <- mapreduce(input=iris.values, 
                 map=map)
iris.mr1 <- from.dfs(mr1)

### simple example with apply
mr2 <- mapreduce(input=iris.values, map=function(k, v) apply(v[,1:4], 2, var))
iris.var <- from.dfs(mr2)
#apply(iris[,1:4], 2, var)

### simple example with tapply
mtcars.values <- to.dfs(mtcars)
mr3 <- mapreduce(input=mtcars.values, map=function(k,v) tapply(v$mpg, list(v$gear, v$cyl), mean))
mtcars.mr <- from.dfs(mr3)
#tapply(mtcars$mpg, list(mtcars$gear, mtcars$cyl),mean)
