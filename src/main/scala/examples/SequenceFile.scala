/**
 * User: hanchensu
 * Date: 2014-02-13
 * Time: 11:58
 */
package examples


import org.apache.spark._
import SparkContext._
import org.apache.hadoop.io.Text

/** Computes an approximation to pi */
object SequenceFile extends Serializable {
	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("SequenceFile")
		val sc = new SparkContext(sparkConf)
		val input = args(0)
		val rawText = sc.sequenceFile(input,classOf[Text],classOf[Text])
		rawText.map(x=> (x._2.toString,(x._1.toString.toInt,1))).reduceByKey((x,y) =>(x._1+y._1,x._2+y._2),50).map(x=>x._2._1.toString+"\t"+x._2._2.toString+"\t"+x._1.toString).saveAsTextFile(args(1))
	}
}

