package Recommendation

import org.apache.spark.rdd.RDD

/**
 * Created by fengzeli on 2014/8/13.
 */
//class SVDFeatureModel(val mean:Double,val userBias:Array[Double],val itemBias:Array[Double],val uFeatureFactor:Array[Array[Double]],val iFeatureFactor:Array[Array[Double]]) extends Serializable{
class SVDFeatureModel(val mean:Double,val userBias:Array[Double],val itemBias:Array[Double],val uFeatureFactor:Array[Array[Double]],val iFeatureFactor:Array[Array[Double]],val lossArray:Array[Double]) extends Serializable{
  def predict(data:RDD[(Int,Int,Array[Double],Array[Double])]):RDD[(Int,Int,Double)]={
    data.map(x=>(x._1+1,x._2+1,compute(x._1,x._2,x._3,x._4)))
  }

  def predict1(data:RDD[(Int,Int,Array[Double],Array[Double],Double)]):RDD[(Int,Int,Double,Double)]={
    data.map(x=>(x._1+1,x._2+1,x._5,compute(x._1,x._2,x._3,x._4)))
  }

  def compute(userid:Int,itemid:Int,userFeature:Array[Double],itemFeature:Array[Double]):Double={
    var finalRating=0.0
    var rating=0.0

    var p = new Array[Double](uFeatureFactor(0).length)
    var q = new Array[Double](iFeatureFactor(0).length)
    for(i <- 0 until userFeature.length){
      for(j <- 0 until uFeatureFactor(i).length)
        //p(j) += userFeature(i)*uFeatureFactor(i)(j)
        p(j) += 1*uFeatureFactor(userFeature(i).toInt)(j)
    }
    for(i <- 0 until itemFeature.length){
      for(j <- 0 until iFeatureFactor(i).length)
        //q(j) += itemFeature(i)*iFeatureFactor(i)(j)
        q(j) += 1*iFeatureFactor(itemFeature(i).toInt)(j)
    }

    var sum=0.0
    for(i <- 0 until p.length){
      sum += p(i)*q(i)
    }
    rating = mean+userBias(userid)+itemBias(itemid)+sum
    val temp_e = Math.exp(-rating)
    finalRating = 1.0/(1.0+temp_e)
    finalRating
    //rating
  }
}

object SVDFeatureModel extends Serializable{
  def getResult(data:RDD[(Int,Int,Array[Double],Array[Double])],mean:Double,userBias:Array[Double],itemBias:Array[Double],uFeatureFactor:Array[Array[Double]],iFeatureFactor:Array[Array[Double]],lossArray:Array[Double]):RDD[(Int,Int,Double)]={
    new SVDFeatureModel(mean,userBias,itemBias,uFeatureFactor,iFeatureFactor,lossArray).predict(data)
  }

  def getResult1(data:RDD[(Int,Int,Array[Double],Array[Double],Double)],mean:Double,userBias:Array[Double],itemBias:Array[Double],uFeatureFactor:Array[Array[Double]],iFeatureFactor:Array[Array[Double]],lossArray:Array[Double]):RDD[(Int,Int,Double,Double)]={
    new SVDFeatureModel(mean,userBias,itemBias,uFeatureFactor,iFeatureFactor,lossArray).predict1(data)
  }
}
