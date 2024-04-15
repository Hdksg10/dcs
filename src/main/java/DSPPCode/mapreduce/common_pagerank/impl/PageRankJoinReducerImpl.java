package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinReducer;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
public class PageRankJoinReducerImpl extends PageRankJoinReducer {
  public void reduce(Text key, Iterable<ReduceJoinWritable> values, Context context)
      throws IOException, InterruptedException
  {
    String page = key.toString();
    String rank = "";
    String links = "";
    for (ReduceJoinWritable value : values) {
      if (value.getTag().equals(ReduceJoinWritable.PAGERNAK))
      {
        rank = value.getData();
      }
      else if (value.getTag().equals(ReduceJoinWritable.PAGEINFO))
      {
        links = value.getData();
      }
    }
    String result = page + " " + rank + " " + links;
    // System.out.println(result);
    context.write(new Text(result), NullWritable.get());
  }
}
