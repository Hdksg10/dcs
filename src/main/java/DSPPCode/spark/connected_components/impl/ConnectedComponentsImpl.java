package DSPPCode.spark.connected_components.impl;

import DSPPCode.spark.connected_components.question.ConnectedComponents;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;




public class ConnectedComponentsImpl extends ConnectedComponents{

  private static JavaPairRDD<String, Integer> toMinRDD(JavaRDD<String> ccRDD) {
    return ccRDD.mapToPair(
        (line) -> {
          String[] nodes = line.split("\\s+");
          String node = nodes[0];
          int minID = Integer.parseInt(node);
          for (int i = 1; i < nodes.length; i++) {
            String n = nodes[i];
            int id = Integer.parseInt(n);
            if (id < minID) {
              minID = id;
            }
          }
          return new Tuple2<>(node, minID);
        }
    );
  }

  public JavaPairRDD<String, Integer> getcc(JavaRDD<String> text)
  {
    JavaRDD<String> ccRDD = text;
    JavaPairRDD<String, Integer> minRDD = toMinRDD(ccRDD);
    boolean isChanged = true; // iter at least once
    while (isChanged){
      JavaPairRDD<String, String> deRDD = ccRDD.flatMapToPair(
          (String line) -> {
            String[] nodes = line.split("\\s+");
            String node = nodes[0];
            ArrayList<Tuple2<String, String>> edges = new ArrayList<>();
            for (int i = 1; i < nodes.length; i++) {
              edges.add(new Tuple2<>(node, nodes[i]));
            }
            // virtual edges
            for (int i = 1; i < nodes.length; i++) {
              for (int j = 1; j < nodes.length; j++) {
                if (i != j) {
                  edges.add(new Tuple2<>(nodes[i], nodes[j]));
                }
              }
            }

            // add self-loop for isolated node
            if (edges.isEmpty()) {
              edges.add(new Tuple2<>(node, node));
            }
            return edges.iterator();
          }
      );
    JavaRDD<String> ccRDD2 = deRDD.groupByKey().map(
        (tuple) -> {
          String node = tuple._1;
          Iterator<String> iter = tuple._2().iterator();
          ArrayList<String> cc = new ArrayList<>();
          while (iter.hasNext()) {
            cc.add(iter.next());
          }
          StringBuilder sb = new StringBuilder();
          for (String s:cc) {
            sb.append(s);
            sb.append(" ");
          }
          return node + " " + sb;
        }
    );
    JavaPairRDD<String, Integer> minRDD2 = toMinRDD(ccRDD2);
    isChanged = isChange(minRDD, minRDD2);
    ccRDD = ccRDD2;
    minRDD = minRDD2;

    }
    return minRDD;
  }
}