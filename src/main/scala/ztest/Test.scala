/**
 * User: hanchensu
 * Date: 2014-02-12
 * Time: 16:07
 */
package ztest
import org.apache.spark.rdd.PairRDDFunctions

object zTest {
	def unapply(input:String) = if(input.length==3) None else Some((input,"_not3"))

	def main(args: Array[String]) {
	   println(Math.log(-1));

	}

}
