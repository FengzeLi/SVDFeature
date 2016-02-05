package Recommendation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.Array
import scala.util.Random
import org.apache.spark.rdd.PairRDDFunctions
import Util.{MLutil, Vectors}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by fengzeli on 2014/7/23.
 */
/**
 * class SVDFeature(val Talpha:Double,val regram:Double,val iters:Int,val globals:Int,val globalFeatrues:Int,val users:Int,
                  val userFeatrues:Int,val items:Int,val itemFeatrues:Int,val userFeatrueFactors:Int,val itemFeatrueFactors:Int) {
*/
class SVDFeature(val Talpha:Double,val regram:Double,val iters:Int,val users:Int,val userFeatrues:Int,val items:Int,val itemFeatrues:Int,val factors:Int) extends Serializable{
  //声明各参数矩阵。
/**
 *var gFeatrueMatrix = Array.fill(globals)(new Array[Double](globalFeatrues))
  var uFeatrueMatrix = Array.fill(users)(new Array[Double](userFeatrues))
  var iFeatrueMatrix = Array.fill(items)(new Array[Double](itemFeatrues))
  var uFeatrueFactorMatrix = Array.fill(userFeatrues)(new Array[Double](userFeatrueFactors))
  var iFeatrueFactorMatrix = Array.fill(itemFeatrues)(new Array[Double](itemFeatrueFactors))
*/
  var userBias = new Array[Double](users)
  var itemBias = new Array[Double](items)
  var uFeatrueFactor = Array.fill(userFeatrues)(new Array[Double](factors))
  var iFeatrueFactor =Array.fill(itemFeatrues)(new Array[Double](factors))
  var totalCount = 0.0 //用于统计样本总数
  var alpha = Talpha //学习率
  var mean = 0.0 //全局评分平均数
  //var item_baseline = new Array[Double](items)//用于记录训练集中对于每一个item的评分平均值，作为衡量模型优劣的baseline.

  //初始化各矩阵函数
  def initialMatrix() {
    var random = new Random()
    random.setSeed(0)
    for (i <- 0 until users) {
      //userBias(i) = random.nextDouble()
      userBias(i)=random.nextGaussian()*Math.sqrt(1.0/regram)
    }

    for (i <- 0 until items) {
      //itemBias(i) = random.nextDouble()
      itemBias(i)=random.nextGaussian()*Math.sqrt(1.0/regram)
    }

    for (i <- 0 until userFeatrues) {
      for (j <- 0 until factors)
        //uFeatrueFactor(i)(j) = random.nextDouble()
        uFeatrueFactor(i)(j)=random.nextGaussian()*Math.sqrt(1.0/regram)
    }

    for (i <- 0 until itemFeatrues) {
      for (j <- 0 until factors)
        //iFeatrueFactor(i)(j) = random.nextDouble()
        iFeatrueFactor(i)(j) = random.nextGaussian()*Math.sqrt(1.0/regram)
    }
  }
/**
    for(i <- 0 until globals){
      for(j <- 0 until globalFeatrues)
        gFeatrueMatrix(i)(j) = random.nextDouble()
    }

    for(i <- 0 until users){
      for(j <- 0 until userFeatrues)
        uFeatrueMatrix(i)(j) = random.nextDouble()
    }

    for(i <- 0 until items){
      for(j <- 0 until itemFeatrues)
        iFeatrueMatrix(i)(j) = random.nextDouble()
    }

    for(i <- 0 until userFeatrues){
      for(j <- 0 until userFeatrueFactors)
        uFeatrueFactorMatrix = random.nextDouble()
    }

    for(i <- 0 until itemFeatrues){
      for(j <- 0 until itemFeatrueFactors)
        iFeatrueFactorMatrix(i)(j) =random.nextDouble()
    }
*/
    //主控流程函数
    def run(data:RDD[(Int,Int,Array[Double],Array[Double],Double)]):SVDFeatureModel={
      initialMatrix()
      totalCount = data.count()
      val totalRating = data.map(x => x._5).reduce((x,y)=>x+y)
      mean = totalRating/totalCount //求出全局平均评分

      /**
      //计算每个item的baseline(用每个item的点击均值算)
      var pairRDD = new PairRDDFunctions(data.map(x=>(x._2,x._5)))
      pairRDD.reduceByKey((x,y)=>x+y).map(x=>item_baseline(x._1)=x._2+10*mean)
      //pairRDD.countByKey().map(x=>item_baseline(x._1)=item_baseline(x._1)/(x._2+10))
      val listRDD = sc
      */
/**
      var grad_gFeatrue = Array.fill(globals)(new Array[Double](globalFeatrues))
      var grad_uFeatrue = Array.fill(users)(new Array[Double](userFeatrues))
      var grad_iFeatrue = Array.fill(items)(new Array[Double](itemFeatrues))
      var grad_uFeatrueFactor = Array.fill(userFeatrues)(new Array[Double](userFeatrueFactors))
      var grad_iFeatrueFactor = Array.fill(itemFeatrues)(new Array[Double](itemFeatrueFactors))
*/
      var grad_userBias = new Array[Double](users)
      var grad_itemBias = new Array[Double](items)
      var grad_uFeatrueFactor = Array.fill(userFeatrues)(new Array[Double](factors))
      var grad_iFeatrueFactor =Array.fill(itemFeatrues)(new Array[Double](factors))
      var judge=1
      var i=0
      var lossTest = new ArrayBuffer[Double]()
      lossTest+=mean
      var oldLoss = getGrad(data,userBias,itemBias,uFeatrueFactor,iFeatrueFactor,grad_userBias,grad_itemBias,grad_uFeatrueFactor,grad_iFeatrueFactor)
      lossTest += oldLoss
      while(i<iters && judge == 1){
        //alpha = 1.0/Math.sqrt(i+1)
        updateArray(userBias,grad_userBias)
        updateArray(itemBias,grad_itemBias)
        updateMatrix(uFeatrueFactor,grad_uFeatrueFactor)
        updateMatrix(iFeatrueFactor,grad_iFeatrueFactor)
        var newLoss = getGrad(data,userBias,itemBias,uFeatrueFactor,iFeatrueFactor,grad_userBias,grad_itemBias,grad_uFeatrueFactor,grad_iFeatrueFactor)
        if(Math.abs(newLoss - oldLoss)<0.001)
          judge = 0
        oldLoss = newLoss
        lossTest += oldLoss
        if(i<=25){
          alpha = alpha * 0.8
        }
        i = i+1
      }
      //lossTest.toArray
      //new SVDFeatureModel(mean,userBias,itemBias,uFeatrueFactor,iFeatrueFactor,lossTest.toArray,item_baseline)
      new SVDFeatureModel(mean,userBias,itemBias,uFeatrueFactor,iFeatrueFactor,lossTest.toArray)
    }

    //求梯度函数，对每个矩阵求矩阵内每个元素的梯度,函数返回目标函数的损失值
    def getGrad(data:RDD[(Int,Int,Array[Double],Array[Double],Double)],userBias:Array[Double],itemBias:Array[Double],uFeatrueFactor:Array[Array[Double]],iFeatrueFactor:Array[Array[Double]],
                grad_userBias:Array[Double],grad_itemBias:Array[Double],grad_uFeatrueFactor:Array[Array[Double]],grad_iFeatrueFactor:Array[Array[Double]]):Double={
      for(i <- 0 until users){
        grad_userBias(i) = 0.0
      }

      for(i <- 0 until items){
        grad_itemBias(i) = 0.0
      }

      for(i <- 0 until userFeatrues){
        for(j <- 0 until factors)
          grad_uFeatrueFactor(i)(j) = 0.0
      }

      for(i <- 0 until itemFeatrues){
        for(j <- 0 until factors)
          grad_iFeatrueFactor(i)(j) = 0.0
      }
      var totalLoss = 0.0

      //计算每条数据的梯度。
      val totalData = data.map{x =>
        var temp_pvector = new Array[Double](factors)
        var temp_qvector = new Array[Double](factors)
        var predict = 0.0
        /**
        for(i <- 0 until userFeatrues){
          for(j <- 0 until factors)
            temp_pvector(j) += x._3(i)*uFeatrueFactor(i)(j)
        }
        for(i <- 0 until itemFeatrues){
          for(j <- 0 until factors)
            temp_qvector(j) += x._4(i)*iFeatrueFactor(i)(j)
        }
        */
        for(i <- 0 until x._3.length){
          for(j <- 0 until factors)
            temp_pvector(j) += 1*uFeatrueFactor(x._3(i).toInt)(j)//这里x._3(i)下标需不需要减一还得回头看一下
        }
        for(i <- 0 until x._4.length){
          for(j <- 0 until factors)
            temp_qvector(j) += 1*iFeatrueFactor(x._4(i).toInt)(j)
        }
        for(i <- 0 until factors)
          predict += temp_pvector(i)*temp_qvector(i)
        predict += mean+userBias(x._1)+itemBias(x._2) //这里mean还没有计算，别忘记
        val trans_predict = 1.0/(1+Math.exp(-predict))

        val temp_e = Math.exp(-predict)//这里用e的幂次方计算e.
        val eps = (x._5+x._5*temp_e-1)/(1+temp_e)
        val temp_userBias = -eps//当前点对相应userid的userBias梯度
        //val temp_userBias = -eps+regram*userBias(x._1)//当前点对相应userid的userBias梯度
        val temp_itemBias = -eps//当前点对相应itemid的itemBias梯度
        //val temp_itemBias = -eps+regram*itemBias(x._2)//当前点对相应itemid的itemBias梯度

        var temp_uFeatureFactor = Array.fill(userFeatrues)(new Array[Double](factors))
        var temp_iFeatureFactor = Array.fill(itemFeatrues)(new Array[Double](factors))
        /**
        for(i <- 0 until userFeatrues){
          for(j <-0 until factors)
            temp_uFeatureFactor(i)(j) = x._3(i)*temp_qvector(j)*eps //计算出p的梯度
        }
        for(i <- 0 until itemFeatrues){
          for(j <- 0 until factors)
            temp_iFeatureFactor(i)(j) = x._4(i)*temp_pvector(j)*eps //计算出q的梯度
        }
        */
        for(i <- 0 until x._3.length){
          for(j <- 0 until factors)
            temp_uFeatureFactor(x._3(i).toInt)(j) = -1*temp_qvector(j)*eps //计算出p的梯度，这里同时要回头看一看x._3(i)是否需要减一
            //temp_uFeatureFactor(x._3(i).toInt)(j) = -1*temp_qvector(j)*eps+regram*uFeatrueFactor(x._3(i).toInt)(j) //计算出p的梯度，这里同时要回头看一看x._3(i)是否需要减一
        }
        for(i <- 0 until x._4.length){
          for(j <- 0 until factors)
            temp_iFeatureFactor(x._4(i).toInt)(j) = -1*temp_pvector(j)*eps//计算出q的梯度
            //temp_iFeatureFactor(x._4(i).toInt)(j) = -1*temp_pvector(j)*eps+regram*iFeatrueFactor(x._4(i).toInt)(j)//计算出q的梯度
        }
        //计算每一个数据点的损失，这里损失函数可能还要再想一下
        /**
        var loss =0.0
        if( x._5 == 0.0){
          loss = (1-x._5)*Math.log(1-trans_predict)
        }else{
          loss = x._5*Math.log(trans_predict)
        }
        */
        //var loss =(1-x._5)*Math.log(1-trans_predict)+x._5*Math.log(trans_predict)

        var loss =0.0
        if(1-trans_predict<0.000001){//这里对损失函数做截断，小于0.000001的使其等于0.000001，防止出现对数0.
          loss = (1-x._5)*Math.log(0.000001)+x._5*Math.log(trans_predict)
        }else if (trans_predict<0.000001){
          loss = (1-x._5)*Math.log(1-trans_predict)+x._5*Math.log(0.000001)
        }else{
          loss =(1-x._5)*Math.log(1-trans_predict)+x._5*Math.log(trans_predict)
        }

        //var loss =(1-x._5)*Math.log(1-trans_predict)+x._5*Math.log(trans_predict)
        loss = -loss
        (x._1,temp_userBias,x._2,temp_itemBias,temp_uFeatureFactor,temp_iFeatureFactor,loss)
      }

      //梯度下降求解，接下来和并各梯度(当前方法没有加入正则化项)
      var pairRDD = new PairRDDFunctions(totalData.map(x=>(x._1,x._2)))
      pairRDD.reduceByKeyLocally((x,y)=>x+y).toArray.map(x=>grad_userBias(x._1)=x._2) //将同一个userid的bais相加
      pairRDD = new PairRDDFunctions(totalData.map(x=>(x._3,x._4)))
      pairRDD.reduceByKeyLocally((x,y)=>x+y).toArray.map(x=>grad_itemBias(x._1)=x._2)//将同一个itemid的bais相加
      val tempuMatirx = totalData.map(x=>x._5).reduce((x,y)=>Vectors.Matrixsum(x,y))
      for(i <- 0 until tempuMatirx.length){
        for (j <- 0 until tempuMatirx(0).length)
          grad_uFeatrueFactor(i)(j) = tempuMatirx(i)(j)
      }
      val tempiMatrix = totalData.map(x=>x._6).reduce((x,y)=>Vectors.Matrixsum(x,y))
      for(i <- 0 until tempiMatrix.length) {
        for (j <- 0 until tempiMatrix(0).length)
          grad_iFeatrueFactor(i)(j) = tempiMatrix(i)(j)
      }
      totalLoss = totalData.map(x=>x._7).reduce((x,y)=>x+y)

      //接下来处理正则化项，当前用的是参数向量的L2范数
      var regularizer = 0.0
      var GradSum = 0.0 //GradSum用来标准化梯度向量
      for(i <- 0 until users){
        //grad_userBias(i) = grad_userBias(i) - regram*userBias(i)
        grad_userBias(i) = grad_userBias(i)/totalCount+ regram*userBias(i)
        GradSum += grad_userBias(i)*grad_userBias(i)
        regularizer += userBias(i)*userBias(i)
      }

      for(i <- 0 until items){
        //grad_itemBias(i) = grad_itemBias(i) - regram*itemBias(i)
        grad_itemBias(i) = grad_itemBias(i)/totalCount + regram*itemBias(i)
        GradSum += grad_itemBias(i) * grad_itemBias(i)
        regularizer += itemBias(i)*itemBias(i)
      }

      for(i <- 0 until userFeatrues){
        for(j <- 0 until factors){
          //grad_uFeatrueFactor(i)(j) = grad_uFeatrueFactor(i)(j) - regram*uFeatrueFactor(i)(j)
          grad_uFeatrueFactor(i)(j) = grad_uFeatrueFactor(i)(j) /totalCount+ regram*uFeatrueFactor(i)(j)
          GradSum += grad_uFeatrueFactor(i)(j)*grad_uFeatrueFactor(i)(j)
          regularizer += uFeatrueFactor(i)(j)*uFeatrueFactor(i)(j)
        }
      }

      for(i <- 0 until itemFeatrues){
        for(j <- 0 until factors){
          //grad_iFeatrueFactor(i)(j) = grad_iFeatrueFactor(i)(j) - regram*iFeatrueFactor(i)(j)
          grad_iFeatrueFactor(i)(j) = grad_iFeatrueFactor(i)(j)/totalCount + regram*iFeatrueFactor(i)(j)
          GradSum += grad_iFeatrueFactor(i)(j)*grad_iFeatrueFactor(i)(j)
          regularizer += iFeatrueFactor(i)(j)*iFeatrueFactor(i)(j)
        }
      }


      //标准化梯度向量
      GradSum = Math.sqrt(GradSum)
      println(GradSum)

      for(i <- 0 until grad_userBias.length){
        grad_userBias(i) = grad_userBias(i)/GradSum
      }

      for(i <- 0  until grad_itemBias.length){
        grad_itemBias(i) = grad_itemBias(i)/GradSum
      }

      for(i <- 0 until userFeatrues){
        for(j <- 0 until factors){
         grad_uFeatrueFactor(i)(j) = grad_uFeatrueFactor(i)(j)/GradSum
        }
      }

      for(i <- 0 until itemFeatrues){
        for(j <- 0 until factors){
           grad_iFeatrueFactor(i)(j) = grad_iFeatrueFactor(i)(j)/GradSum
        }
      }

      //totalLoss = totalLoss + 0.5 * regram * regularizer
      totalLoss = totalLoss /totalCount+ 0.5 * regram * regularizer
      /**
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
      */
      totalLoss
    }
/**
    //根据现有的mean、userBias、itemBias、uFeatureFactor、iFeatureFactor计算当前数据的平凡的函数
    def compute(userid:Int,itemid:Int,userfeature:Array[Double],itemfeature:Array[Double]):Double={
      var temp_ufeature = new Array[Double](factors)
      var temp_ifeature = new Array[Double](factors)
      var predict = 0.0
      for(i <- 0 until userFeatrues){
        for (j <- 0 until factors)
           temp_ufeature(j) += userfeature(i)*uFeatrueFactor(i)(j)
      }
      for(i <- 0 until itemFeatrues){
        for(j <- 0 until factors)
          temp_ifeature(j) += itemfeature(i)*iFeatrueFactor(i)(j)
      }
      for(i <- 0 until factors)
        predict += temp_ufeature(i)*temp_ifeature(i)
      predict += mean + userBias(userid) + itemBias(itemid)
      predict
    }
*/
    //矩阵更新函数
    def updateMatrix(matrix:Array[Array[Double]],grad:Array[Array[Double]]){
      for(i <- 0 until matrix.length){
        for(j <- 0 until matrix(i).length)
            matrix(i)(j) -= alpha * grad(i)(j)
      }
    }

    def updateArray(bais:Array[Double],grad:Array[Double]){
      for(i <- 0 until bais.length)
        bais(i) -= alpha*grad(i)
    }

}


object SVDFeature extends Serializable{
  def train(data:RDD[(Int,Int,Array[Double],Array[Double],Double)],Talpha:Double,regram:Double,iters:Int,users:Int,userFeatrues:Int,items:Int,itemFeatrues:Int,factors:Int):SVDFeatureModel={
    new SVDFeature(Talpha,regram,iters,users,userFeatrues,items,itemFeatrues,factors).run(data)
  }

  //dealNoFeatureData函数主要是将没有特征的数据转换成指定的数据格式，然后可以套进SVDFeature模型中去，这样原来的模型就退化成SVD了。
  def dealNoFeatureData(data:RDD[(Int,Int,Int,Int,Int,Int,String,Int,Double)]):RDD[(Int,Int,Array[Double],Array[Double],Double)]={
    val dealedData = data.map{x =>
      /**
      var uFeature = new Array[Double](users)
      var iFeature = new Array[Double](items)
      uFeature(x._1-1) = 1 //这里注意下标要减一
      iFeature(x._2-1) = 1
      */
      var uFeature = new Array[Double](1)
      var iFeature = new Array[Double](1)
      uFeature(0) = x._1-1 //这里注意下标要减一
      iFeature(0) = x._2-1
      (x._1-1,x._2-1,uFeature,iFeature,x._9)//这里也注意要把userid和itemid都减一
    }
    dealedData
  }

  //对测试数据的转换
  def dealTestNoFeatureData(data:RDD[(Int,Int,Int,Int,Int,Int,String,Int)]):RDD[(Int,Int,Array[Double],Array[Double])]={
    val dealedData = data.map{x =>
      /**
      var uFeature = new Array[Double](users)
      var iFeature = new Array[Double](items)
      uFeature(x._1-1) = 1 //这里注意下标要减一
      iFeature(x._2-1) = 1
        */
      var uFeature = new Array[Double](1)
      var iFeature = new Array[Double](1)
      uFeature(0) = x._1-1 //这里注意下标要减一
      iFeature(0) = x._2-1
      (x._1-1,x._2-1,uFeature,iFeature)//这里也注意要把userid和itemid都减一
    }
    dealedData
  }

  //dealFeatureData将含有特征信息的数据转换成指定数据格式。
  def dealFeatureData(data:RDD[(Int,Int,Int,Int,Int,Int,String,Int,Double)]):RDD[(Int,Int,Array[Double],Array[Double],Double)]={
    val dealedData = data.map{x =>
      /**
      var uFeature = new Array[Double](3469)
      var iFeature = new Array[Double](23)
      uFeature(x._3) = 1//标注性别信息
      uFeature(2+x._4) = 1//标注年龄信息
      uFeature(9+x._5) = 1//标注职业信息
      uFeature(30+x._6) = 1//标注邮编信息
      val genres = x._7.trim().split("|")
      for(i <- 0 until genres.length ){
        iFeature(genres(i).toInt) = 1 //标注电影体裁信息
      }
      iFeature(18+x._8) = 1//标注电影评论时间信息
      */
      var uFeature = new ArrayBuffer[Double]()
      var iFeature = new ArrayBuffer[Double]()
      uFeature += x._3 //标注性别信息
      uFeature += x._4+2 //标注年龄信息
      uFeature += x._5+9 //标注职业信息
      uFeature += x._6+30 //标注邮编信息
      uFeature.toArray
      val genres = x._7.trim().split("|")
      for(i <- 0 until genres.length ){
        if(genres(i)!="" && genres(i)!="|"){ //这里有机会想一想为什么trim和split过后还会出现“”或“|”
          iFeature += genres(i).toInt //标注电影体裁信息
        }
      }
      iFeature += x._8+18 //标注电影评论时间信息
      iFeature.toArray
      (x._1-1,x._2-1,uFeature.toArray,iFeature.toArray,x._9)
    }
    dealedData
  }

  //对测试数据的转换
  def dealTestFeatureData(data:RDD[(Int,Int,Int,Int,Int,Int,String,Int)]):RDD[(Int,Int,Array[Double],Array[Double])]={
    val dealedData = data.map{x =>
      /**
      var uFeature = new Array[Double](3469)
      var iFeature = new Array[Double](23)
      uFeature(x._3) = 1//标注性别信息
      uFeature(2+x._4) = 1//标注年龄信息
      uFeature(9+x._5) = 1//标注职业信息
      uFeature(30+x._6) = 1//标注邮编信息
      val genres = x._7.trim().split("|")
      for(i <- 0 until genres.length ){
        iFeature(genres(i).toInt) = 1 //标注电影体裁信息
      }
      iFeature(18+x._8) = 1//标注电影评论时间信息
        */
      var uFeature = new ArrayBuffer[Double]()
      var iFeature = new ArrayBuffer[Double]()
      uFeature += x._3 //标注性别信息
      uFeature += x._4+2 //标注年龄信息
      uFeature += x._5+9 //标注职业信息
      uFeature += x._6+30 //标注邮编信息
      uFeature.toArray
      val genres = x._7.trim().split("|")
      for(i <- 0 until genres.length ){
        if(genres(i)!="" && genres(i)!="|"){ //这里有机会想一想为什么trim和split过后还会出现“”或“|”
          iFeature += genres(i).toInt //标注电影体裁信息
        }
      }
      iFeature += x._8+18 //标注电影评论时间信息
      iFeature.toArray
      (x._1-1,x._2-1,uFeature.toArray,iFeature.toArray)
    }
    dealedData
  }

  def main (args: Array[String]) {
    val sc = new SparkContext(args(0),"SVDFeature",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass).toSeq)
    val metaData = Util.MLutil.loadSVDFeatureData(sc,args(1))
    val oldData = metaData._1
    val users = metaData._2
    val items = metaData._3
    //将原始数据转换成不含特征的训练数据
    //val data = dealNoFeatureData(oldData,users,items)
    //val data = dealNoFeatureData(oldData)
    //将原始数据转换成含特征的训练数据
    val data = dealFeatureData(oldData)
    data.persist()
    var iters = args(2).toInt
    var alpha = args(3).toDouble
    var regram = args(4).toDouble
    var factors = args(5).toInt
    var storeDir = args(6)
    //根据不含特征的训练数据训练模型
    //val model = SVDFeature.train(data,alpha,regram,iters,users,users,items,items,factors)
    //val model = SVDFeature.train(data,alpha,regram,iters,6040,6040,3952,3952,factors)
    //根据含有特征的训练数据训练模型
    val model = SVDFeature.train(data,alpha,regram,iters,6040,3469,3952,23,factors)
    val modelMatirxP = model.uFeatureFactor
    val modelMatrixQ = model.iFeatureFactor

    var item_baseline = new Array[Double](3952)//用于记录训练集中对于每一个item的评分平均值，作为衡量模型优劣的baseline.
    val pairRDD = new PairRDDFunctions(data.map(x=>(x._2,x._5)))
    pairRDD.reduceByKeyLocally((x,y)=>x+y).toArray.map(x=>item_baseline(x._1)=x._2+10*model.mean)
    pairRDD.countByKey().toArray.map(x=>item_baseline(x._1)=item_baseline(x._1)/(x._2+10))
    MLutil.storemodel(sc,storeDir+"/item_baseline",item_baseline)

    /**
    //存储不含特征的训练模型,此时模型退化为SVD
    val userFeatureFactor = new Array[(Int,Array[Double])](6040)
    val itemFeatureFactor = new Array[(Int,Array[Double])](3952)
    //for(i <- 0 until users)
    for(i <- 0 until 6040)
      userFeatureFactor(i) = Tuple2(i+1,modelMatirxP(i))
    //for(i <-0 until items)
    for(i <- 0 until 3952)
      itemFeatureFactor(i) = Tuple2(i+1,modelMatrixQ(i))
    */

    //存储含有特征的训练模型
    val userFeatureFactor = new Array[(Int,Array[Double])](3469)
    val itemFeatureFactor = new Array[(Int,Array[Double])](23)
    for(i <- 0 until 3469)
      userFeatureFactor(i) = Tuple2(i,modelMatirxP(i))
    for(i <-0 until 23)
      itemFeatureFactor(i) = Tuple2(i,modelMatrixQ(i))


    println("The number of users:   "+users)
    println("The number of items:   "+items)
    sc.makeRDD(userFeatureFactor).map(x=>x._1+","+x._2.mkString(" ")).saveAsTextFile(storeDir+"/userFeatureFactor")
    sc.makeRDD(itemFeatureFactor).map(x=>x._1+","+x._2.mkString(" ")).saveAsTextFile(storeDir+"/itemFeatureFactor")
    MLutil.storemodel(sc,storeDir+"/userBias",model.userBias)
    MLutil.storemodel(sc,storeDir+"/itemBias",model.itemBias)
    //MLutil.storemodel(sc,storeDir+"/item_baseline",item_baseline)

    val loss_array = model.lossArray
    sc.makeRDD(loss_array).map(x=>Tuple1(x)).saveAsTextFile(storeDir+"/lossArray")
    //接下来对预测集进行预测，并把结果存储到HDFS.
    /**
    val testMetaData = Util.MLutil.loadTestData(sc,args(7))
    //将原始数据转换成不含特征的训练数据
    //val testData = dealTestNoFeatureData(testMetaData)
    //将原始数据转换成含特征的训练数据
    val testData = dealTestFeatureData(testMetaData)
    */
    val testMetaData = Util.MLutil.loadSVDFeatureData(sc,args(7))
    val testData = dealFeatureData(testMetaData._1)
    //val testData = dealNoFeatureData(testMetaData._1)
    val testDataCount = testData.count()
    testData.persist()
    val prediction = SVDFeatureModel.getResult1(testData,model.mean,model.userBias,model.itemBias,model.uFeatureFactor,model.iFeatureFactor,model.lossArray)
    //prediction.map(x=>Tuple1(x)).saveAsTextFile(storeDir+"/prediction")

    //对比预测结果和baseline(用均值预测)的损失
    var predictLoss = 0.0
    var baselineLoss = 0.0
    val predictionWithLoss = prediction.map{x=>
      var loss = 0.0
      if(1-x._4<0.000001){
        loss = (1-x._3)*Math.log(0.000001)+x._3*Math.log(x._4)
      }else if(x._4<0.000001){
        loss = (1-x._3)*Math.log(1-x._4)+x._3*Math.log(0.000001)
      }else{
        loss = (1-x._3)*Math.log(1-x._4)+x._3*Math.log(x._4)
      }

      //var loss = (1-x._3)*Math.log(1-x._4)+x._3*Math.log(x._4)
      var lossByMean = 0.0
      if(item_baseline(x._2-1)==0.0){
        lossByMean=(1-x._3)*Math.log(1-model.mean)+x._3*Math.log(model.mean)
      }else{
        lossByMean=(1-x._3)*Math.log(1-item_baseline(x._2-1))+x._3*Math.log(item_baseline(x._2-1))
      }
      loss = -loss
      lossByMean = -lossByMean
      (x._1,x._2,x._3,x._4,loss,lossByMean)
    }
    predictionWithLoss.map(x=>Tuple1(x)).saveAsTextFile(storeDir+"/prediction")
    predictLoss = predictionWithLoss.map(x=>x._5).reduce((x,y)=>x+y)
    predictLoss = predictLoss/testDataCount
    baselineLoss = predictionWithLoss.map(x=>x._6).reduce((x,y)=>x+y)
    baselineLoss = baselineLoss/testDataCount
    var lossCompare = new ArrayBuffer[Double]()
    lossCompare+=predictLoss
    lossCompare+=baselineLoss
    sc.makeRDD(lossCompare.toArray).map(x=>Tuple1(x)).saveAsTextFile(storeDir+"/lossCompare")
    sc.stop()
  }
}

