package spark.basic.example

import org.apache.spark.SparkContext
import spark.basic.init.InitSpark

object RDDExample extends InitSpark {
  private val dataExample: Seq[(String, Int)] = Seq(
    ("Lee", 2000),
    ("Park", 10000),
    ("Kim", 5000),
    ("Choi", 1000),
    ("Jeong", 7500),
    ("Song", 2500)
  )

  private val groupDataExample: Seq[(String, Int)] = Seq(
    ("Kim", 2000),
    ("Kim", 10000),
    ("Lee", 5000),
    ("Lee", 1000),
    ("Park", 7500),
    ("Park", 2500)
  )

  def filterExample(minValue: Int): Array[(String, Int)] = {
    sc.parallelize(dataExample)
      .filter(x => x._2 >= minValue)
      .collect()
  }

  def mapExample(): Array[String] = {
    sc.parallelize(dataExample)
      .map(x => x._1 + ":" + x._2)
      .collect()
  }

  def flatMapExample(): Array[Char] = {
    sc.parallelize(dataExample)
      .filter(x => x._2 >= 10000)
      .flatMap(x => x._1)
      .collect()
  }

  def countExample(): Long = {
    sc.parallelize(dataExample)
      .count()
  }

  def reduceByKeyExample(): Array[(String, Int)] = {
    sc.parallelize(groupDataExample)
      .reduceByKey((x, y) => x + y)
      .collect()
  }

  def reduceExample(): Int = {
    sc.parallelize(dataExample)
      .map(x => x._2)
      .reduce((x, y) => x + y)
  }
}
