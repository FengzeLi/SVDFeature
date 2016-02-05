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
//object GlomTest {
//	def main(args: Array[String]) {
//
//		val master=args(0)
//		val sc = new SparkContext(master, "WordCount",
//			System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
//
//
//		val input1 = args(1)
//		val input2 = args(2)
//
////		val input = "./wc_input"
////		val output = "./wc_output"
//
//		val rdd1 = sc.textFile(input1)
//		val rdd2 = sc.textFile(input2)
//
//
//
//		val res = rdd1.repartition(4).glom().collect()
//
////		val res = rdd1.map(x => {
////			val a = rdd2.map(y => (y,x)).collect()
////			a.length
////		}).collect()
//		println(res.length)
//		for(z <- res) {
//			for(l <- z) {
//				print(l+",")
//			}
//			println()
//		}
//
//		val res2 = rdd2.cartesian(rdd1.repartition(4).glom()).groupByKey().collect()
//		for(z<-res2) {
//			print(z._1+":")
//			println(z._2.map(x=>x.mkString(",")).mkString("__"))
//		}
////		val result = textFile.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _)
////		val result = rdd.filter(line => line.length < 6000)
//
////		result.saveAsTextFile(output)
//	}
//}
