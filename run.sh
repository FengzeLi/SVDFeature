#!/bin/sh
export SPARK_USER_CLASSPATH=/home/adrd/hanchensu/ideaProjects/spark-client-apps/target/spark-client-apps-1.0-SNAPSHOT.jar
spark-class examples.WordCount target/spark-client-apps-1.0-SNAPSHOT.jar