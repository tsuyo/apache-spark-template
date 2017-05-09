/* WordCountJavaApp.java */
import java.util.*;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;

public class WordCountJavaApp {
  public static void main(String[] args) {
    String inputFile = "/opt/spark-2.1.1-bin-hadoop2.7/README.md";
    String outputFile = "output_wordcount_java";
    SparkConf conf = new SparkConf().setAppName("WordCount Java Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> input = sc.textFile(inputFile);
    JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
      public Iterator<String> call(String s) { return Arrays.asList(s.split(" ")).iterator(); }
    });
    JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) { return new Tuple2(s, 1); }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer x, Integer y) { return x + y; }
    });
    counts.saveAsTextFile(outputFile);

    sc.stop();
  }
}
