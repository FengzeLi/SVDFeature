SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly_2.10-0.9.1-hadoop2.2.0-cdh5.0.0-beta-2.jar \
spark-class org.apache.spark.deploy.yarn.Client \
--jar target/spark-client-apps-1.0-SNAPSHOT.jar  \
--class examples.WordCount \
--args yarn-standalone \
--num-workers 3 \
--master-memory 2g \
--worker-memory 2g \
--worker-cores 1
