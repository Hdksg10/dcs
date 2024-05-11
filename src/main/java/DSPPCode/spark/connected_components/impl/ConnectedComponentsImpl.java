package DSPPCode.spark.connected_components.impl;

import DSPPCode.spark.connected_components.question.ConnectedComponents;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Int;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;



public class ConnectedComponentsImpl extends ConnectedComponents{
  private static ArrayList<Integer> bfs (int start, HashMap<Integer,
      ArrayList<Integer>> nodes,
      HashMap<Integer, Boolean> isVisited){
      ArrayList<Integer> cc = new ArrayList<>();
      ArrayList<Integer> queue = new ArrayList<>();
      cc.add(start);
      queue.add(start);
      isVisited.put(start, true);
      while (!queue.isEmpty()) {
        int node = queue.get(0);
        queue.remove(0);
        ArrayList<Integer> linkNodes = nodes.get(node);
        for (int linkNode: linkNodes) {
          if (!isVisited.containsKey(linkNode)) {
            cc.add(linkNode);
            queue.add(linkNode);
            isVisited.put(linkNode, true);
          }
        }
      }
      return cc;
  }

  public JavaPairRDD<String, Integer> getcc(JavaRDD<String> text)
  {
    JavaRDD<String> ccRDD = text;
    boolean isChanged = true; // iter at least once
    while (isChanged){
      JavaRDD<String> new_ccRDD = ccRDD.mapPartitions(
          iterator -> {
            List<String> local_cc = new ArrayList<>();
            HashMap<Integer, Boolean> isVisited = new HashMap<>();
            HashMap<Integer, ArrayList<Integer>> nodes = new HashMap<>();
            while (iterator.hasNext()) {
              String line = iterator.next();
              String [] nodes_str = line.split("\\s+");
              int node1 = Integer.parseInt(nodes_str[0]);
              ArrayList<Integer> linkNodes = new ArrayList<>();
              for (int i = 1; i < nodes_str.length; i++){
                linkNodes.add(Integer.parseInt(nodes_str[i]));
              }
              nodes.put(node1, linkNodes);
            }
            for (int node: nodes.keySet()) {
              if (!isVisited.containsKey(node)) {
                ArrayList<Integer> cc = bfs(node, nodes, isVisited);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < cc.size(); i++) {
                  sb.append(cc.get(i));
                  if (i != cc.size() - 1) {
                    sb.append(" ");
                  }
                }
                local_cc.add(sb.toString());
              }
            }
            return local_cc.iterator();
          }
      );
      
    }
    return null;
  }
}