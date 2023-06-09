/*
 * This Scala Testsuite was generated by the Gradle 'init' task.
 */
package spark.basic

import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import spark.basic.example.RDDExample

@RunWith(classOf[JUnitRunner])
class AppSuite extends AnyFunSuite {
  test("RDDExampleTest") {
    assert(RDDExample.filterExample(5000).length == 3)
    assert(RDDExample.mapExample().contains("Lee:2000"))
    assert(RDDExample.flatMapExample().contains('P'))
    assert(RDDExample.countExample() == 6)
    assert(RDDExample.reduceByKeyExample().contains(("Kim", 12000)))
    assert(RDDExample.reduceExample() == 28000)
  }
}
