package util

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 搜索相关性评分算法
  *
  * @author Mengsi He
  */
class TSES_bm25(docs: RDD[String]) extends Serializable {

  private val k1 = 1.5f
  private val b = 0.75f
  //文档句子的个数
  private val D = docs.count()
  //单词总数
  val wordSum: Double = docs.flatMap(_.split(" ")).count()
  //文档句子的平均长度
   val avgdl: Double = wordSum / D
  //文档中每个句子中的每个词与词频
   val f = docs.map {
    x =>
      val f = x.split(" ")
        .foldLeft(Map.empty[String, Int]) {
          // foldLeft从左开始计算，然后往右遍历,Map.empty[String, Int]创建空的可变映射
          (count, word) => count + (word -> (count.getOrElse(word, 0) + 1)) // getOrElse（key,default）获取key对应的value，如果不存在则返回一个默认值。
        }
      f
  }
    //f.foreach(println)
  //文档中全部词语与出现在几个句子中
   val words = docs
    .flatMap { file =>
      var map = mutable.Map[String, Int]()
      val words = file.split(" ").iterator
      while (words.hasNext) map += (words.next() -> 1)
      map
    }
  private val df = words.reduceByKey(_ + _).map(x => {
    x._1 + " " + x._2
  })
  //df.foreach(println)
  //判断一个词与一个文档的相关性权重wi
  private val idf = df.map { x =>
    val word = x.split(" ")(0)
    val num = x.split(" ")(1).toDouble
    val w = Math.log((D - num + 0.5) / (num + 0.5))
    (word, w)
  }

  val Farray=f.toArray()
  val IDFarray=idf.toArray()
  def simAll(sentence: String) = {
    val scoreArray = new Array[Double](D.toInt)
    for (i <- 0 until D.toInt) {
      scoreArray(i) = TransDF(Farray(i), IDFarray, sentence)

    }
    scoreArray
  }

  def TransDF(f: Map[String, Int], idf: Array[(String, Double)], sentence: String): Double = {
    var transIDF = mutable.Map[String, Double]()
    for ((k, v) <- idf) {
      transIDF += (k -> v)
    }

    var score: Double = 0.0
    for (word <- sentence.split(" ")) {
      if (f.contains(word)) //是共有的词，则看成对句子之间的相似度有贡献
      {
        val dl = f.size     //该句子的长度
        val wf = f.get(word).get //获取word在该句子中的词频
      val idfValue = transIDF.get(word).get //获取word与一个文档的相关性的权重
        score += (idfValue * wf * (k1 + 1)
          / (wf + k1 * (1 - b + b * dl
          / avgdl)))
      }
    }
    score
  }

}
