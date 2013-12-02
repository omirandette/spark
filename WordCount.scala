import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import scala._
import scala.Predef._
import SparkContext._
/**
 * Created by oliviermirandette on 12/1/2013.
 */


object SparkWordCount1 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.foreach(println)
  }
}
object SparkWordCount2 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.flatMap(line => line.split("[\\s]")).foreach(println)
  }
}
object SparkWordCount3 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.flatMap(line => line.split("[\\s]"))
    .filter(word => !word.isEmpty).foreach(println)
  }
}
object SparkWordCount4 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.flatMap(line => line.split("[\\s]"))
      .map(word => word.trim)
      .filter(word => !word.isEmpty).foreach(println)
  }
}
object SparkWordCount5 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => word.trim.toLowerCase)
      .map(word => regex.replaceAllIn(word,""))
      .filter(word => !word.isEmpty).foreach(println)
  }
}
object SparkWordCount6 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => word.trim)
      .map(word => regex.replaceAllIn(word,""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _).foreach(println)
  }
}
object SparkWordCount7 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => word.trim)
      .map(word => regex.replaceAllIn(word,""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true).foreach(println)
  }
}
object SparkWordCount8 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => word.trim)
      .map(word => regex.replaceAllIn(word,""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true).foreach(println)
  }
}
object SparkWordCount9 {

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r

    val result = book
      .flatMap(line => line.split("[\\s]"))
      .map(word => word.trim.toLowerCase)
      .map(word => regex.replaceAllIn(word,""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true)

    result.persist(StorageLevel.MEMORY_ONLY)

    result.map(tuple => tuple._2).foreach(tuple => Unit)

    result.filter(tuple => tuple._1 > 1000)
      .map(tuple => (tuple._2.charAt(0),1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true)
      .foreach(tuple => Unit)
  }
}


