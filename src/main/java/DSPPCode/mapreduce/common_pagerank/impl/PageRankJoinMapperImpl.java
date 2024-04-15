package DSPPCode.mapreduce.common_pagerank.impl;
import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinMapper;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class PageRankJoinMapperImpl extends PageRankJoinMapper{
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
  {
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    boolean isRank = fileName.contains("rank");
    String[] values = value.toString().split("\\s+");
    ReduceJoinWritable reduceJoinWritable = new ReduceJoinWritable();
    if (isRank) {
      reduceJoinWritable.setTag(ReduceJoinWritable.PAGERNAK);
      reduceJoinWritable.setData(values[1]);
    } else {
      reduceJoinWritable.setTag(ReduceJoinWritable.PAGEINFO);
      StringBuilder sb = new StringBuilder();
      for (int i = 1; i < values.length; i++) {
        sb.append(values[i]);
        if (i != values.length - 1) {
          sb.append(" ");
        }
      }
      reduceJoinWritable.setData(sb.toString());
    }
    context.write(new Text(values[0]), reduceJoinWritable);
  }
}
