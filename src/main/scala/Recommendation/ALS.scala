package Recommendation


import Util.MLutil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.Array
import scala.util.Random
import org.apache.spark.rdd.PairRDDFunctions
import Util.Vectors
/**
 * Created with IntelliJ IDEA.
 * User: dell
 * Date: 14-7-1
 * Time: 上午10:58
 * To change this template use File | Settings | File Templates.
 */
class ALS(val Talpha:Double,val regram:Double,val niters:Int,var features:Int,var nuser:Int,var nitem:Int) extends Serializable{

  var userfeature = Array.fill(nuser)(new Array[Double](features))
  var itemfeature = Array.fill(nitem)(new Array[Double](features))
  var Bfeature = new Array[Double](features)
  var totalcount = 0.0
  var alpha = Talpha

  def initialfeatures(){
    var random = new Random()
    random.setSeed(0)
    for(i <- 0 until nuser){
      for(j <- 0 until features)
        userfeature(i)(j) = random.nextDouble()
    }
    for(i <- 0 until nitem){
      for(j <- 0 until features)
        itemfeature(i)(j) = random.nextDouble()
    }
    for(j <- 0 until features)
      Bfeature(j) = random.nextDouble()
  }
  def run(data:RDD[(Int,Int,Double)]):MatrixModel={
    initialfeatures()
    totalcount = data.count()
    var grad_user =  Array.fill(nuser)(new Array[Double](features))
    var grad_item =   Array.fill(nitem)(new Array[Double](features))
    var grad_B =  new Array[Double](features)
    var judge = 1
    var i = 0
    var oldloss = getGradUser(data,userfeature,itemfeature,Bfeature,grad_user,grad_item,grad_B)
    while(i < niters && judge == 1){
      update(userfeature,grad_user)
      update(itemfeature,grad_item)
      update(Bfeature,grad_B)
      var newloss = getGradUser(data,userfeature,itemfeature,Bfeature,grad_user,grad_item,grad_B)
      if(Math.abs(newloss - oldloss) < 0.001)
        judge = 0
      oldloss = newloss
      alpha = alpha * 0.96
    }
    new MatrixModel(userfeature,itemfeature,Bfeature)
  }

  def getGradUser(data:RDD[(Int,Int,Double)],userfeature:Array[Array[Double]],itemfeature:Array[Array[Double]],Bfeature:Array[Double],
                  grad_user:Array[Array[Double]],grad_item:Array[Array[Double]],grad_B:Array[Double]):Double={
    for(i <- 0 until nuser){
      for(j <- 0 until features)
        grad_user(i)(j) = 0.0
    }
    for(i <- 0 until nitem){
      for(j <- 0 until features)
        grad_item(i)(j) = 0.0
    }
    for(j <- 0 until features)
      grad_B(j) = 0.0
    var totalloss = 0.0

    val totaldata = data.map{x => //这里map的话可以对所有的数据进行transformation.
      var temp_user = new Array[Double](features)
      var temp_item = new Array[Double](features)
      var temp_B = new Array[Double](features)
      var eps =  compute(userfeature(x._1),itemfeature(x._2),Bfeature)-x._3//表示求偏导前面共同的部分

      for(i <- 0 until features) {
        temp_user(i) = Bfeature(i) * itemfeature(x._2)(i)*eps//求出U中一维向量（即一个user向量）的梯度
        temp_item(i) = Bfeature(i) * userfeature(x._1)(i)*eps//求出V中一维向量（即一个item向量）的梯度
        temp_B(i) = userfeature(x._1)(i)* itemfeature(x._2)(i)*eps//求出S这个一维向量的梯度（因为S为对角阵，所以这里可以用一维向量表示）
      }
      (x._1,temp_user,x._2,temp_item,temp_B,0.5*eps*eps)//这里返回的数据结构中包括了一个user对应的V中梯度向量，一个item对应的S中的梯度向量，以及当前点对应二次损失函数中的残差
    }

    var pairRDD = new PairRDDFunctions( totaldata.map(x => (x._1,x._2)))
    pairRDD.reduceByKey((x,y) => Vectors.Arraysum(x,y)).map(x => grad_user(x._1)=x._2)//这里reduce将上面计算出来的相同userid的梯度加在一起，并且记录入grad_user矩阵中Userid对应的行
    pairRDD = new PairRDDFunctions(totaldata.map(x => (x._3,x._4)))
    pairRDD.reduceByKey((x,y) => Vectors.Arraysum(x,y)).map(x => grad_item(x._1) = x._2)//同理，这里reduce将上面计算出来的相同itemid的梯度加在一起，并且记录入grad_item矩阵中的itemid对应的行
    var temp_B = totaldata.map(x=>x._5).reduce((x,y)=>Vectors.Arraysum(x,y))//这里将每个点计算出来的S矩阵向量梯度求和（梯度下降）
    for(i <- 0 until features)
      grad_B(i) = temp_B(i)//将梯度记录入grad_B
    totalloss = totaldata.map(x => x._6).reduce((x,y) => x+y)//reduce算出当前所有点的残差和。
    var loss = 0.0
    for(i <- 0 until nuser){
      for(j <- 0 until features){
        grad_user(i)(j) = grad_user(i)(j)/totalcount + regram * userfeature(i)(j)
        loss += userfeature(i)(j)*userfeature(i)(j)
      }

    }
    for(i <- 0 until nitem){
      for(j <- 0 until features){
        grad_item(i)(j) = grad_item(i)(j)/totalcount + regram * itemfeature(i)(j)
        loss += itemfeature(i)(j)*itemfeature(i)(j)
      }

    }
    for(j <- 0 until features) {
      grad_B(j) =   grad_B(j)/totalcount +regram * Bfeature(j)
      loss += Bfeature(j)*Bfeature(j)
    }
    totalloss = totalloss/totalcount + 0.5 * regram * loss
    totalloss
  }
  //根据现有的U、S、V求出一个预测评分
  def compute(userfeature:Array[Double],productfeature:Array[Double],Bfeature:Array[Double]):Double={
    var sum = 0.0
    for(i <- 0 until features)
      sum += userfeature(i)*productfeature(i)*Bfeature(i)
    sum
  }
  def update(feature:Array[Array[Double]],grad:Array[Array[Double]]){
    for(i <- 0 until feature.length){
      for(j <- 0 until feature(i).length)
        feature(i)(j)  -= alpha * grad(i)(j)
    }
  }
  def update(feature:Array[Double],grad:Array[Double]){
    for(i <- 0 until feature.length)
      feature(i) -= alpha * grad(i)
  }
}



object ALS extends Serializable{

  def train(data:RDD[(Int,Int,Double)],alpha:Double,regram:Double,niters:Int,features:Int,nuser:Long,nitem:Long):MatrixModel={
    new ALS(alpha,regram,niters,features,nuser.toInt,nitem.toInt).run(data)
  }
  def main(args:Array[String]){
    val sc = new SparkContext(args(0),"ALS",System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
    val f= MLutil.loadALSdata(sc,args(1))
    val data = f._3
    data.persist()
    var niters = args(2).toInt
    var alpha = args(3).toDouble
    var features = args(4).toInt
    var mulu = ""
    var nuser:Long = 0
    var nitem:Long = 0
    var regram = 0.0
    if(args.length == 7){
      nuser = data.map(x => x._1).distinct().count
      nitem = data.map(x => x._2).distinct().count
      regram = args(5).toDouble
      mulu = args(6)
    }  else{
      nuser = args(5).toLong
      nitem = args(6).toLong
      regram = args(7).toDouble
      mulu = args(8)

    }
    val model = ALS.train(data,alpha,regram,niters,features,nuser,nitem)
    val usermap = f._1
    val itemmap = f._2
    val userfeatures = new Array[(Int,Array[Double])](nuser.toInt)
    val itemfeatures = new Array[(Int,Array[Double])](nitem.toInt)
    val modeluser = model.userfeatures
    val modelitem = model.productFeatures
    for(i <- 0 until nuser.toInt)
      userfeatures(i) = Tuple2(usermap.get(i.toString).get.toInt,modeluser(i))
    for(i <- 0 until nitem.toInt)
      itemfeatures(i) = Tuple2(itemmap.get(i.toString).get.toInt,modelitem(i))
    println("userlength               "+nuser)
    println("itemlength             "+nitem)
    sc.makeRDD(userfeatures).map(x=>x._1+","+x._2.mkString(" ")).saveAsTextFile(mulu+"/userfeatures")
    sc.makeRDD(itemfeatures).map(x=>x._1+","+x._2.mkString(" ")).saveAsTextFile(mulu+"/productfeatures")
    MLutil.storemodel(sc,mulu+"/Bfeatures",model.B)
    sc.stop()
  }
}
