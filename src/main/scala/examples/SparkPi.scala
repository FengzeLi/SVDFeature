///**
// * User: hanchensu
// * Date: 2014-02-13
// * Time: 11:58
// */
//package examples
//
//
//import org.apache.hadoop.io.Text
//import org.apache.spark._
//import SparkContext._
//
///** Computes an approximation to pi */
//object SparkPi {
//	def main(args: Array[String]) {
//		if (args.length == 0) {
//			System.err.println("Usage: SparkPi <master> [<slices>]")
//			System.exit(1)
//		}
//		val spark = new SparkContext(args(0), "SparkPi",
//			System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
//		val slices = if (args.length > 1) args(1).toInt else 2
//		val n = 100000 * slices
//		val count = spark.parallelize(1 to n, slices).map(x => (x, 1)).reduceByKey(_ + _).count()
//		println("Pi is roughly " + 4.0 * count / n)
//		spark.stop()
//	}
//}
//
