spark-submit --class org.apache.spark.examples.SparkPi --master spark://JNCDH5slave-72-53:7077 --deploy-mode client /opt/spark-1.0.0-cdh5.0.2/examples/target/spark-examples_2.10-1.0.0.jar 10 
#spark-submit --class org.apache.spark.examples.SparkPi --master yarn-cluster --deploy-mode client  /opt/spark-1.0.0-cdh5.0.2/examples/target/spark-examples_2.10-1.0.0.jar 100000 
