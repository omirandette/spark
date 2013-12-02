import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import scala._
import scala.Predef._
import SparkContext._
/**
 * Created by oliviermirandette on 12/1/2013.
 */


object SparkWordCount1 {
  /*
    reading the file and printing every line
   */

  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.foreach(println)
  }
}
object SparkWordCount2 {
  /*
    reading the file and printing every word
   */
  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.flatMap(line => line.split("[\\s]")).foreach(println)
  }
}
object SparkWordCount3 {
  /*
    reading the file, generate words, filter out "empty" word and print each word
   */
  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.flatMap(line => line.split("[\\s]"))
      .filter(word => !word.isEmpty).foreach(println)
  }
}
object SparkWordCount4 {
  /*
    reading the file, generate words, trim each word, filter out "empty" word and print each word
   */
  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    book.flatMap(line => line.split("[\\s]"))
      .map(word => word.trim)
      .filter(word => !word.isEmpty).foreach(println)
  }
}
object SparkWordCount5 {
  /*
    reading the file, generate words, trim each word, put in lower case,
      replace special char, filter out "empty" word and print each word
   */
  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => regex.replaceAllIn(word.trim.toLowerCase, ""))
      .filter(word => !word.isEmpty).foreach(println)
  }
}
object SparkWordCount6 {

  def main(args : Array[String]) {
    /*
      reading the file, generate words, trim each word, put in lower case,
      replace special char, filter out "empty", count each word and print the word and the count
    */
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => regex.replaceAllIn(word.trim.toLowerCase, ""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _).foreach(println)
  }
}

object SparkWordCount7 {
  /*
    reading the file, generate words, trim each word, put in lower case,
      replace special char, filter out "empty", count each word, sort by count ASC and print the word and the count
  */
  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => regex.replaceAllIn(word.trim.toLowerCase, ""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true).foreach(println)
  }
}

object SparkWordCount8 {
  /*
    reading the file, generate words, trim each word, put in lower case,
      replace special char, filter out "empty", count each word, sort by count ASC and print the word and the count
  */
  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r
    book.flatMap(line => line.split("[\\s]"))
      .map(word => regex.replaceAllIn(word.trim.toLowerCase, ""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true).foreach(println)
  }
}


object SparkWordCount9 {
  /*
    reading the file, generate words, replace special char, filter out "empty", count each word, sort by count ASC
     store the result in memory
     print the word and the count
     print the word only
     print the number of word that start with each char for only the word with more than 1000 occurrences. The result is sorted by count ASC
  */
  def main(args : Array[String]) {
    val context = new SparkContext("local", "test")
    val book = context.textFile("/Users/oliviermirandette/Downloads/pg135.txt")
    val regex = "[,.:;'\"\\?\\-!\\(\\)]".r

    val result = book
      .flatMap(line => line.split("[\\s]"))
      .map(word => regex.replaceAllIn(word.trim.toLowerCase, ""))
      .filter(word => !word.isEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true)

    result.persist(StorageLevel.MEMORY_ONLY)
    result.foreach(println)

    result.map(tuple => tuple._2).foreach(println)

    result.filter(tuple => tuple._1 > 1000)
      .map(tuple => (tuple._2.charAt(0),tuple._1))
      .reduceByKey(_ + _)
      .map(tuple => (tuple._2, tuple._1))
      .sortByKey(true)
      .foreach(println)
  }
}


