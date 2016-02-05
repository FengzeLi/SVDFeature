package Util

/**
 * Created with zhukunguang.
 * User: dell
 * Date: 14-5-3
 * Time: 下午1:13
 * To change this template use File | Settings | File Templates.
 */
object Vectors {

  def trasfertoArray(line:String): Array[Double] = {
    var tokens = line.trim().split("\t")
    var array = new Array[Double](tokens.length)
    array(0)= 0
    for(i <- 1 until tokens.length){
      array(i) =   tokens(i).toDouble
    }
    array
  }
  def trasferbayestoArray(line:String): Array[Double] = {
    var tokens = line.trim().split(",")

    var array = new Array[Double](tokens.length-1)

    for(i <- 1 until tokens.length){

      array(i-1) = tokens(i).toDouble
    }
    array
  }
  def Arraysum(a:Array[Double],b:Array[Double]): Array[Double] = {
    var c :Array[Double] = new Array[Double](a.length)

    for(i <- 0 until a.length){
      c(i) = a(i) + b(i)
    }
    c
  }
  def Offanshu(array:Array[Double]):Double={
    var result = 0.0
    result = math.sqrt(array.map(x => x*x).sum)
    result
  }
  def Arraydiff(a:Array[Double],b:Array[Double]):Array[Double]={
    var c = new Array[Double](a.length)
    for(i<- 0 until c.length){
      c(i) = a(i) - b(i)
    }
    c
  }
  def ArrayMulti(a:Array[Double],b:Array[Double]):Double={
    var sum = 0.0
    for(i <- 0 until a.length){
      sum = sum + a(i)*b(i)
    }
    sum

  }
  def getMaxIndex(prob:Array[Double]):Double={

    var max = prob(0)
    var k = 0
    for(i <- 1 until prob.length){
      if(prob(i) > max){
        max = prob(i)
        k = i
      }
    }
    k.toDouble
  }
  def getSecondMaxIndex(prob:Array[Double]):Double={

    var max = prob(0)
    var k = 0
    for(i <- 1 until prob.length){
      if(prob(i) > max){
        max = prob(i)
        k = i
      }
    }
    max = -1
    var j = 0
    for(i <- 1 until prob.length){
      if(i != k.toInt){
        if(prob(i) > max){
          max = prob(i)
          j = i
        }
      }
    }
    j.toDouble
  }
  def getthirdMaxIndex(prob:Array[Double],k:Int):Double={
    var tempprob = prob.clone()
    var index  = 0
    var max = 0.0
    for(j <- 0 until k){
      for(i <- j until prob.length){
        if(tempprob(i) > tempprob(j)){
          max = tempprob(i)
          index = i
        }
      }
      tempprob(index) = tempprob(j)
      tempprob(j) = max
    }
    tempprob(k-1)
  }
  def Matrixsum(a:Array[Array[Double]],b:Array[Array[Double]]): Array[Array[Double]] = {
    var c :Array[Array[Double]] = Array.fill(a.length)(new Array[Double](a(0).length))

    for(i <- 0 until a.length){
      for(j <- 0 until a(0).length)
        c(i)(j) = a(i)(j) + b(i)(j)
    }
    c
  }
  def oushijuli(a1:Array[Double],a2:Array[Double]):Double={
    var distance = 0.0;
    for(i <- 0 until a1.length){
      distance = distance + Math.pow(a1(i)-a2(i),2)
    }
    return distance
  }
}
