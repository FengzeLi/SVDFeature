package Util

/**
 * Created with zhukunguang.
 * User: dell
 * Date: 14-5-3
 * Time: 下午12:53
 * To change this template use File | Settings | File Templates.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.io.{File, FileReader, BufferedReader, PrintWriter}
import scala.Array
import scala.Tuple2
object MLutil {


  def loadALSdata(sc:SparkContext,dir:String):(Map[String,String],Map[String,String],RDD[(Int,Int,Double)])={

    var usermap = Map[String,String]()
    var itemmap = Map[String,String]()
    var Fuser = Map[String,String]()
    var Fitem = Map[String,String]()
    var data = sc.textFile(dir).map{x =>
      var tokens = x.split(",")
      (tokens(0).toInt,tokens(1).toInt,tokens(2).toDouble)
    }
    var array = data.map(x => x._1).distinct().collect()

    for(i <- 0 until array.length){
      usermap += (array(i).toString -> i.toString )
      Fuser += (i.toString -> array(i).toString)
    }
    var array1 = data.map(x => x._2).distinct().collect()
    for(i <- 0 until array1.length){
      itemmap += (array1(i).toString -> i.toString )
      Fitem += (i.toString -> array1(i).toString)
    }

    data = data.map(x => (usermap.get(x._1.toString).get.toInt,itemmap.get(x._2.toString).get.toInt,x._3))
    (Fuser,Fitem,data)
  }

  def loadSVDFeatureData(sc:SparkContext,dataDir:String):(RDD[(Int,Int,Int,Int,Int,Int,String,Int,Double)],Int,Int)={
    val data=sc.textFile(dataDir,20).map{x =>
      val tokens = x.trim().split(" ")
      (tokens(0).toInt,tokens(1).toInt,tokens(2).toInt,tokens(3).toInt,tokens(4).toInt,tokens(5).toInt,tokens(6),tokens(7).toInt,tokens(8).toDouble)
    }
    val users = data.map(x => x._1).distinct().count().toInt
    val items = data.map(x => x._2).distinct().count().toInt
    (data,users,items)
  }

  def loadTestData(sc:SparkContext,dataDir:String):(RDD[(Int,Int,Int,Int,Int,Int,String,Int)])={
    val data=sc.textFile(dataDir,20).map{x =>
      val tokens = x.trim().split(" ")
      (tokens(0).toInt,tokens(1).toInt,tokens(2).toInt,tokens(3).toInt,tokens(4).toInt,tokens(5).toInt,tokens(6),tokens(7).toInt)
    }
    data
  }

  def storemodel(sc:SparkContext,dir:String,array:Array[Double]) {
    sc.makeRDD(array.map(x =>(1,x))).groupByKey().map(x => x._2.mkString(",")).saveAsTextFile(dir)
  }

}


