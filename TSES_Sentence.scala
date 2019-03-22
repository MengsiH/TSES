package util

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by Mengsi He on 2017/3/8.
  */
class TSES_Sentence extends Serializable {
  /**
    * BM25相似度
    */
  private var bm25: TSES_bm25 = null
  /**
    * 阻尼系数，一般取值为0.85
    */
  private val d: Double = 0.85
  /**
    * 最大迭代次数
    */
  private val max_iter: Int = 200
  private val min_diff: Double = 0.001

  def getsummary(line: RDD[String], size: Double) = {
    val sentenceList = line.flatMap(_.split("[\n\r，\".,。:：“”？?！!；;]"))
      .map(_.trim).filter(!_.isEmpty).filter(_.nonEmpty) //将文章分割为句子
    val docs = sentenceList.map { x => //拆分为[句子[单词]]形式的文档
      val xStr: String = x
      val list = IKSegmenterTool.participle(xStr) //分词处理
      list.toString.replace(" ", "").replace("[", "")
        .replace("]", "").replace(",", " ")//将词性标志过滤
    }
    docs.foreach(println)
    val D = docs.count().toDouble
    val Size=if(D<10) 1 else D*size
   // val S=if(Size>)

    this.bm25 = new TSES_bm25(docs) //BM25相似度
    val tses_Sentence = tsesSentence(docs.toArray()) //All sentences 权重迭代并升序后的结果（权重，index）
    val sentenIndexResult = getTopIndex(tses_Sentence, Size.toInt) //size个顺序与原文一致的关键句Index
    val gettopSentence = getTopSentence(sentenceList, sentenIndexResult)
    var result=new String
    for (top<-gettopSentence){
         result+=top
    }
    result
  }

  def getTopSentence(sentenList: RDD[String], sentenIndexResult: Array[Int]) = {

    val zipSentence = sentenList.zipWithIndex().toArray()
    val result=new Array[String](sentenIndexResult.size)
    var cnt=0
    for (sentence <- zipSentence) {
      for (index <- sentenIndexResult)
        if (sentence._2 == index) {
          if(cnt==sentenIndexResult.size-1){
            result(cnt)=sentence._1+"。"
          }else{
         result(cnt)=sentence._1+","
          }
          cnt+=1
        }
    }
    result
  }

  def getTopIndex(tses_Sentence: List[(Double, Int)], size: Int): Array[Int] = {
    val indexResult = new Array[Int](size) //size个关键句的index
    var tses_size = tses_Sentence.size - 1
    //得到关键句index
    for (index <- 0 until size) {
      indexResult(index) = tses_Sentence(tses_size)._2
      tses_size -= 1
    }
    val sortIndexResult = indexResult.sorted //让关键句的顺序与原文一致
    sortIndexResult
  }

  def tsesSentence(docs: Array[String]) = {
    val D = docs.size
    val weight = new Array[Array[Double]](D)   //句子和其他句子的相关程度
    val weight_sum = new Array[Double](D)    //该句子和其他句子相关程度之和
    var vertex = new Array[Double](D)    //迭代之后收敛的权重
    var cnt = 0

    for (sentence <- docs) {
      val scores: Array[Double] = bm25.simAll(sentence)
      weight(cnt) = scores
      weight_sum(cnt) = scores.sum - scores(cnt)       // 减掉自己，自己跟自己肯定最相似
      vertex(cnt) = 1.0
      cnt += 1
    }
//    for (i<-weight){
//      for(x<-i)
//      print(x+",  ")
//      println()
//    }
//    println("weight")
//    weight_sum.foreach(println)
    breakable {
      for (_ <- 0 until max_iter) {
        val m = new Array[Double](D)
        var max_diff = 0.0
        for (i <- 0 until D) {        //TextRank公式
          m(i) = 1 - d
          for (j <- 0 until D) {
            if (!(i == j || weight_sum(j) == 0))
              m(i) += (d * weight(j)(i) / weight_sum(j) * vertex(j))
          }
          val diff: Double = Math.abs(m(i) - vertex(i))
          if (diff > max_diff)
            max_diff = diff
        }
        vertex = m
        if (max_diff <= min_diff) break
      }
    }
    // 我们来排个序吧
    var top = mutable.Map[Double, Int]()
    var num = 0
    for (x <- vertex) {
      top += (x -> num)
      num += 1
    }
    val sortTop = top.toList.sorted
    sortTop
  }
}


