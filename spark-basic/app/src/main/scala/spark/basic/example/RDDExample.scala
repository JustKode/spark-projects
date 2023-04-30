package spark.basic.example

import org.apache.spark.SparkContext

class RDDExample {
  private val dataExample: Seq[(String, Int)] = Seq(
    ("Lee", 2000),
    ("Park", 10000),
    ("Kim", 5000),
    ("Choi", 1000),
    ("Jeong", 7500),
    ("Song", 2500)
  )

  def operationExample(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(dataExample)

    println("filter second key >= 5000")
    rdd1.repartition(2)
      .filter(x => x._2 >= 5000)
      .foreach(x => println(x))

    println("==================")

    println("map vs flatMap")

    val rdd2 = sc.parallelize(dataExample)

    rdd2.map(x => x._1)
      .foreach(x => println(x))

    val rdd3 = sc.parallelize(dataExample)

    rdd3.flatMap(x => x._1)
      .foreach(x => println(x))
  }
}
