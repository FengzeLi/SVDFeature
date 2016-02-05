///**
// * User: hanchensu
// * Date: 2014-02-12
// * Time: 13:38
// */
//package examples
//
//import org.apache.spark.{SparkConf, SparkContext}
//import SparkContext._
//
//object WordCount2 {
//	def main(args: Array[String]) {
//
////		val spark = new SparkContext(args(0), "SparkPi",
////			System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
////
////		val a = Array("abc asdasd","bcda asdasda")
////		val rdd = spark.parallelize(a,1)
////
////
////		val res = rdd.flatMap(line => line.split(" "))
////		  .map(word => (word, 1))
////		  .reduceByKey(_ + _).count()
////
////		println(res)
////
////		println("Pi is roughly " + 4.0 * res)
////		spark.stop()
//	}
//}
