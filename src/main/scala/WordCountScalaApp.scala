/* WordCountScalaApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCountScalaApp {
  def main(args: Array[String]) {
    val inputFile = "/opt/spark-2.1.1-bin-hadoop2.7/README.md"
    val outputFile = "output_wordcount_scala"
    val conf = new SparkConf().setAppName("WordCount Scala Application")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case(x, y) => x + y }
    counts.saveAsTextFile(outputFile)
    sc.stop()
  }
}
