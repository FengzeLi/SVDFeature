///**
// * User: hanchensu
// * Date: 2014-02-12
// * Time: 13:38
// */
//package examples
//
//import org.apache.spark.{SparkConf, SparkContext}
//import SparkContext._
//object WordCount {
//	def main(args: Array[String]) {
//
////		System.setProperty("spark.executor.memory","8g")
////		System.setProperty("spark.local.dir","/tmp,/data-b/spark-tmp,/data-b/spark-tmp1")
//
//		val master=args(0)
////		val sc = new SparkContext(master, "WordCount",
////			System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
//
//		val sc = new SparkContext(master, "WordCount",
//			System.getenv("SPARK_HOME"), List(args(2)))
//
//
//		val input = args(1)
////		val output = args(2)
//
//		val rdd = sc.textFile(input)
//
////		val a = Array("abc asdasd","bcda asdasda")
////		val rdd = sc.parallelize(a,1)
//
//
//		val res = rdd.flatMap(line => line.split(" "))
//		  .map(word => (word, 1))
//		  .reduceByKey(_ + _).count()
//
//		println(res)
//
//		sc.stop()
//	}
//}
