hadoop fs -rmr /user/root/fengzeli/training_set_dealed
spark-submit --class examples.MovieLensALS --master  spark://JNCDH5slave-72-53:7077 --deploy-mode client --executor-memory 15g --driver-memory 2g --executor-cores 16 --num-executors 8 --jars target/scopt_2.10-3.2.0.jar target/spark-client-apps-1.0-SNAPSHOT.jar --rank 10 --numIterations 1 --lambda 1.0  /user/adrd/fengzeli/training_set_dealed /user/root/fengzeli/output101
#spark-submit --class examples.MovieLensALS --master  spark://JNCDH5slave-72-53:7077 --deploy-mode client --executor-memory 10g --driver-memory 2g --executor-cores 16 --num-executors 8 --jars target/scopt_2.10-3.2.0.jar target/spark-client-apps-1.0-SNAPSHOT.jar --rank 10 --numIterations 20 --lambda 1.0 --kryo  /user/adrd/fengzeli/training_set_dealed /user/root/fengzeli/output101
