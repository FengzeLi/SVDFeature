package Recommendation

import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 * User: dell
 * Date: 14-7-1
 * Time: 上午10:58
 * To change this template use File | Settings | File Templates.
 */
class MatrixModel(val userfeatures:Array[Array[Double]],val productFeatures:Array[Array[Double]],val B:Array[Double]) {

  def predict(data:RDD[(Int,Int)]):RDD[(Int,Int,Double)]={
    data.map(x => (x._1,x._2,compute(x._1,x._2)))
  }
  def compute(x:Int,y:Int):Double={
    var sum = 0.0
    for(i <- 0 until userfeatures(0).length)
      sum += userfeatures(x)(i)*productFeatures(x)(i)*B(i)
    sum
  }
}

