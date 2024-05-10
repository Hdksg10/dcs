package DSPPCode.spark.eulercircuit.impl;

import DSPPCode.spark.eulercircuit.question.EulerCircuit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class EulerCircuitImpl extends EulerCircuit {
  public boolean isEulerCircuit(JavaRDD<String> lines, JavaSparkContext jsc) {
    JavaRDD<String> edges = lines.flatMap((String line) -> Arrays.asList(line.split("\\s+")).iterator());
    JavaPairRDD<String, Integer> num_edges = edges.mapToPair((String node) -> new Tuple2<>(node, 1));
    JavaPairRDD<String, Integer> degree = num_edges.reduceByKey(Integer::sum);
    JavaPairRDD<String, Integer> odd_degree = degree.filter((Tuple2<String, Integer> tuple) -> tuple._2 % 2 == 1);
    return odd_degree.count() == 0;
  }
}