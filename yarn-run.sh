
#export SPARK_YARN_USER_ENV="SPARK_LIBRARY_PATH=/opt/adrdhadoop/hadoop/lib/native:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/native,SPARK_CLASSPATH=/opt/adrdhadoop/hadoop/share/hadoop/common/lib/hadoop-lzo-0.4.15-gplextras5.0.0-beta-2-SNAPSHOT.jar:/opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/hadoop-lzo.jar"

#export SPARK_YARN_APP_JAR=/opt/develop/adrd/suhanchen/mllib-test/target/mllib-0.1.0.jar
#export SPARK_YARN_APP_JAR=target/spark-client-apps-1.0-SNAPSHOT.jar

#用spark-class方式来运行，SPARK_YARN_APP_JAR就乱指一个就行，关键是SPARK_CLASSPATH最后计算结果有没有用户jar包
export SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly_2.10-0.9.1-hadoop2.2.0-cdh5.0.0-beta-2.jar 
export SPARK_YARN_APP_JAR=target/spark-client-apps-1.0-SNAPSHOT_1.jar
export SPARK_USER_CLASSPATH=$SPARK_YARN_APP_JAR

#export SPARK_YARN_APP_JAR=$SPARK_HOME/examples/target/scala-2.10/spark-examples_2.10-assembly-0.9.1.jar
#export SPARK_USER_CLASSPATH=$SPARK_HOME/examples/target/scala-2.10/spark-examples_2.10-assembly-0.9.1.jar


#### env ####
export SPARK_WORKER_MEMORY=7g
export SPARK_MASTER_MEMORY=2g
export SPARK_WORKER_CORES=8
export SPARK_WORKER_INSTANCES=8
export SPARK_YARN_APP_NAME=yarn-client-test


#### which app ####
#run-example org.apache.spark.examples.SparkPi yarn-client
spark-class examples.WordCount yarn-client /user/adrd/ctr-test-spark/output/owlqn_features bcd.jar
#MASTER=yarn-client spark-shell
