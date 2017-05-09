/* SimpleJavaApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleJavaApp {
  public static void main(String[] args) {
    String logFile = "/opt/spark-2.1.1-bin-hadoop2.7/README.md";
    SparkConf conf = new SparkConf().setAppName("Simple Java Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", Lines with b: " + numBs);

    sc.stop();
  }
}
