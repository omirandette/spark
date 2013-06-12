/**
 * Created with IntelliJ IDEA.
 * User: olivier
 * Date: 10/06/13
 * Time: 3:31 PM
 * To change this template use File | Settings | File Templates.
 */

import scala._
import scala.Predef._
import spark.SparkContext
import SparkContext._

object TutorialSpark {

  def main(args: Array[String]) {
    val context = new SparkContext("local", "test")
    val list = List((1, 1), (1, 2), (3, 2), (3, 5), (4, 1), (4, 2), (4, 5))
    val rdd = context.parallelize(list)


    def empty(a: Any) {
      a // put your breakpoint here
    }
    /** **************************************
      * Reduce By Key
      */
    def tutorialReduceByKey(a: Int, b: Int): Int = {
      val c = a * b
      c
    }

    rdd.reduceByKey(tutorialReduceByKey).foreach(empty)
    rdd.reduceByKey(_ * _).foreach(empty)

    // return (1,2) (3,10) (4,10)
    /*
       Groub By, SortByKey, GroupByKey
     */

    def tutorialGroupBy(a: (Int, Seq[Int])): (Int, Int) = {
      val (key, values) = a
      var total = 1
      for (num <- values) {
        total *= num
      }
      (key, total)
    }

    val groupByKey = rdd.groupByKey()
    groupByKey.foreach(empty)

    val groupBy = groupByKey.groupBy(tutorialGroupBy)
    groupBy.foreach(empty)

    groupBy.sortByKey().foreach(empty)

    // return (1,2) (3,10) (4,10)

    /*
      filter
     */

    def tutorialFilter(tuple: (Int, Int)): Boolean = {
      val (a, b) = tuple
      if (b > a) {
        true
      } else {
        false
      }
    }

    rdd.filter(tutorialFilter).foreach(empty)
    rdd.filter(tuple => {
      val (a, b) = tuple
      if (b > a) {
        true
      } else {
        false
      }
    }).foreach(empty)
    // return (1,2) (3,5) (4,5)


    /*
       union
    */
    val list2 = List((1, 1), (1, 2), (3, 2), (3, 5), (4, 1), (4, 2), (4, 6))
    val rdd2 = context.parallelize(list2)
    val sum = rdd.union(rdd2)
    sum.foreach(empty)
    // return (1,1),(1,2),(3,2),(3,5), (4,1), (4,2), (4,5), (1,1),(1,2),(3,2),(3,5), (4,1), (4,2), (4,6)

    /*
    distinct
     */

    sum.distinct().foreach(empty)

    // return (1,1),(1,2),(3,2),(3,5), (4,1), (4,2), (4,5), (4,6)


    /*
    Join
    */

    rdd.join(rdd2).foreach(empty)

    /* return
    (1,(1,1))
    (1,(1,2))
    (1,(2,1))
    (1,(2,2))
    (3,(2,2))
    (3,(2,5))
    (3,(5,2))
    (3,(5,5))
    (4,(1,1))
    (4,(1,2))
    (4,(1,6))
    (4,(2,1))
    (4,(2,2))
    (4,(2,6))
    (4,(5,1))
    (4,(5,2))
    (4,(5,6))
    */

    /*
 cogroup
 */

    rdd.cogroup(rdd2).foreach(empty)

    /* return
    (1,(ArrayBuffer(1, 2),ArrayBuffer(1, 2)))
    (3,(ArrayBuffer(2, 5),ArrayBuffer(2, 5)))
    (4,(ArrayBuffer(1, 2, 5),ArrayBuffer(1, 2, 6)))
    */

    /*
       cartesiaon
    */
    rdd.cartesian(rdd2).foreach(println)

    /*
    ((1,1),(1,1))
((1,1),(1,2))
((1,1),(3,2))
((1,1),(3,5))
((1,1),(4,1))
((1,1),(4,2))
((1,1),(4,6))
((1,2),(1,1))
((1,2),(1,2))
((1,2),(3,2))
((1,2),(3,5))
((1,2),(4,1))
((1,2),(4,2))
((1,2),(4,6))
((3,2),(1,1))
((3,2),(1,2))
((3,2),(3,2))
((3,2),(3,5))
((3,2),(4,1))
((3,2),(4,2))
((3,2),(4,6))
...... and many more
     */

  }

}
