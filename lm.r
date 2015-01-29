# Defining data variables
#X = matrix(rnorm(2000), ncol = 10)
#y = as.matrix(rnorm(200))
# Bundling data variables into dataframe
X = matrix(c(3,6,10,12),4,1)
y = matrix(c(3.1,3.6,3.8,4),4,1)
smodel <- lm(y~x)
summary(smodel)

##############################################
# Defining the datasets with Big Data matrix X
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming.jar")
Sys.setenv(JAVA_HOME="/usr/java/jdk1.7.0_67-cloudera")

library(rJava)
library(rhdfs)
library(rmr2)

#test rhdfs
hdfs.init()

#X = matrix(rnorm(20000), ncol = 10)
#X.index = to.dfs(cbind(1:nrow(X), X))
#y = as.matrix(rnorm(2000))

X = matrix(c(3,6,10,12),4,1)
X.index = to.dfs(cbind(1:nrow(X), X))
y = matrix(c(3.1,3.6,3.8,4),4,1)

# Function defined to be used as reducers
Sum = function(., YY){
  keyval(1, list(Reduce('+', YY)))
}

XtX =
values(
  # For loading hdfs data in to R
  from.dfs(
    # MapReduce Job to produce XT*X
    mapreduce(
      input = X.index,
      # Mapper – To calculate and emitting XT*X
      map =
        function(., Xi) {
          yi = y[Xi[,1],]
          Xi = Xi[,-1]
          keyval(1, list(t(Xi) %*% Xi))},
      # Reducer – To reduce the Mapper output by performing 
      # sum operation over them
      reduce = Sum,
      combine = TRUE)))[[1]]

Xty = values(
  # For loading hdfs data
  from.dfs(
    # MapReduce job to produce XT * y
    mapreduce(
      input = X.index,
      # Mapper – To calculate and emitting XT*y
      map = function(., Xi) {
        yi = y[Xi[,1],]
        Xi = Xi[,-1]
        keyval(1, list(t(Xi) %*% yi))},
      # Reducer – To reducer the Mapper output by performing 
      # sum operation over them
      reduce = Sum, 
      combine = TRUE)))[[1]]

solve(XtX, Xty)
solve(XtX) %*% Xty

