package Main

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import util.{KeywordExtractor, TSES_Sentence}

/*
* */
object AbstractExtract {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("TextSummary")
    val sc = new SparkContext(conf)
    for (i <- 1 until 2) {
       val filePath= "E:\\TextDemo\\TextRank.txt"   //"E:\\TextDemo\\SogouC\\sogou ("+i+").txt"
      val line = sc.hadoopFile(filePath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
        .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))
      println("result"+i)
      TSESrun(line)
    }
    sc.stop()
  }
  def TSESrun(line: RDD[String]) {
    val summary = new TSES_Sentence
    val result = summary.getsummary(line, 0.1) //指定需要提取几个句子
    result.foreach(print)
    println(" ")
  }

}
