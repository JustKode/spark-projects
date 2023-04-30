package spark.basic.init

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait InitSpark {
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark Example")
    .master("local")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val sqlContext: SQLContext = spark.sqlContext

  private def init = {
    sc.setLogLevel("ERROR")
  }

  init

  def close = {
    spark.close()
  }
}
