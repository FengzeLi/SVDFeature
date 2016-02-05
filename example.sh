#### required env ####
export SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly_2.10-0.9.1-hadoop2.2.0-cdh5.0.0-beta-2.jar 
export SPARK_YARN_APP_JAR=yourJar
export SPARK_USER_CLASSPATH=$SPARK_YARN_APP_JAR

#### env ####
export SPARK_WORKER_MEMORY=4g
export SPARK_MASTER_MEMORY=2g
export SPARK_WORKER_CORES=2
export SPARK_WORKER_INSTANCES=8
export SPARK_YARN_APP_NAME=spark-yarn-test


#### run app ####
spark-class yourClass yarn-client arg1 arg2 ..... 
