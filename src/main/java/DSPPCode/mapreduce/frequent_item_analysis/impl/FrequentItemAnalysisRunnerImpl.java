package DSPPCode.mapreduce.frequent_item_analysis.impl;
import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisRunner;
import DSPPCode.mapreduce.frequent_item_analysis.impl.CombinerImpl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.net.URISyntaxException;

public class FrequentItemAnalysisRunnerImpl extends FrequentItemAnalysisRunner{
  public void configureMapReduceTask(Job job)
      throws IOException, URISyntaxException
  {
    // job.setJarByClass(FrequentItemAnalysisRunnerImpl.class);
    // job.setMapperClass(FrequentItemAnalysisMapperImpl.class);
    // job.setReducerClass(FrequentItemAnalysisReducerImpl.class);
    // job.setMapOutputKeyClass(Text.class);
    // job.setMapOutputValueClass(IntWritable.class);
    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(NullWritable.class);
    job.setCombinerClass(CombinerImpl.class);
    return;
  }
}
