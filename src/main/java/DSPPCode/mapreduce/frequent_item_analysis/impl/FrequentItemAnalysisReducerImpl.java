package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FrequentItemAnalysisReducerImpl extends FrequentItemAnalysisReducer{
  public void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException
  {
    int total = context.getConfiguration().getInt("count.of.transactions", 0);
    double support = context.getConfiguration().getDouble("support", 0.0);
    int count = 0;
    for (IntWritable value : values) {
      count += value.get();
    }
    String keyStr = key.toString();
    keyStr = keyStr.replaceAll(" ", ",");
    if (count >= total * support) {
      context.write(new Text(keyStr), NullWritable.get());
    }
  }
}
