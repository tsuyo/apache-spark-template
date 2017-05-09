/* SimpleScalaApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleScalaApp {
  def main(args: Array[String]) {
    val logFile = "/opt/spark-2.1.1-bin-hadoop2.7/README.md"
    val conf = new SparkConf().setAppName("Simple Scala Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
