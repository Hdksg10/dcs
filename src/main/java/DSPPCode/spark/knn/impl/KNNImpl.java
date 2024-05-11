package DSPPCode.spark.knn.impl;

import DSPPCode.spark.knn.question.Data;
import DSPPCode.spark.knn.question.KNN;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import scala.reflect.ClassTag;

public class KNNImpl extends KNN{

  private static double euclideanDistance(double[] x1, double[] x2) {
    assert x1.length == x2.length;
    double sum = 0;
    for (int i = 0; i < x1.length; i++) {
      sum += Math.pow(x1[i] - x2[i], 2);
    }
    return Math.sqrt(sum);
  }

  public KNNImpl(int k) {
    super(k);
  }

  public JavaPairRDD<Data, Data> kNNJoin(JavaRDD<Data> trainData, JavaRDD<Data> queryData) {
    // JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkSession.getActiveSession().get().sparkContext());
    // // List<Data> queryDataList = queryData.collect();
    // final Broadcast<JavaRDD<Data>> queryDataBroadcast = jsc.broadcast(queryData);
    trainData = trainData.cache();
    SparkSession ss = SparkSession.builder().getOrCreate();
    SparkContext sc = ss.sparkContext();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
    List<Data> queryDataList = queryData.collect();
    final Broadcast<List<Data>> queryDataBroadcast = jsc.broadcast(queryDataList);

    return trainData.flatMapToPair(
        line -> {
          List<Tuple2<Data, Data>> result = new ArrayList<>();
          List<Data> queryDataBroadcastList = queryDataBroadcast.value();
          for (Data data : queryDataBroadcastList) {
            result.add(new Tuple2<>(line, data));
          }
          return result.iterator();
        }
    ).cache();

    // return trainData.cartesian(queryDataBroadcast.getValue());
  }

  public JavaPairRDD<Integer, Tuple2<Integer, Double>> calculateDistance(JavaPairRDD<Data, Data> data) {
    return data.mapToPair(
        tuple -> {
          Data trainData = tuple._1;
          Data queryData = tuple._2;
          double distance = euclideanDistance(trainData.x, queryData.x);
          return new Tuple2<>(queryData.id, new Tuple2<>(trainData.y, distance));
        }
    );
  }

  public JavaPairRDD<Integer, Integer> classify(JavaPairRDD<Integer, Tuple2<Integer, Double>> data) {
    return data.groupByKey().mapToPair(
        tuple -> {
          int queryId = tuple._1;
          Iterable<Tuple2<Integer, Double>> distances = tuple._2;
          ArrayList<Tuple2<Integer, Double>> distancesList = new ArrayList<>();
          for (Tuple2<Integer, Double> distance : distances) {
            distancesList.add(distance);
          }
          distancesList.sort((x, y) -> {
            return Double.compare(x._2, y._2);
          });
          HashMap<Integer, Integer> count = new HashMap<>();
          for (int i = 0; i < k; i++) {
            int label = distancesList.get(i)._1;
            if (count.containsKey(label)) {
              count.put(label, count.get(label) + 1);
            }
            else {
              count.put(label, 1);
            }
          }

          int maxCount = -1;
          int maxLabel = -1;
          for (int label : count.keySet()) {
            if (count.get(label) > maxCount) {
              maxCount = count.get(label);
              maxLabel = label;
            }
            if (count.get(label) == maxCount && label < maxLabel) {
              maxLabel = label;
            }
          }
          return new Tuple2<>(queryId, maxLabel);
        }
    );
  }
}
