package DSPPCode.mapreduce.transitive_closure.question;

import DSPPCode.mapreduce.transitive_closure.impl.TransitiveClosureMapperImpl;
import DSPPCode.mapreduce.transitive_closure.impl.TransitiveClosureReducerImpl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;

import java.io.InputStreamReader;
import java.io.BufferedReader;

public class TransitiveClosureRunner extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), getClass().getSimpleName());
    // 设置程序的类名
    job.setJarByClass(getClass());

    // 设置数据的输入输出路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // // 读取表头并广播给Reduce Task
    // Configuration conf = job.getConfiguration();
    // FileSystem fs = FileSystem.get(conf);
    // Path path = new Path(args[0]);
    // BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    // String header = br.readLine();
    // br.close();
    //
    // job.getConfiguration().set("header", header);

    // 设置map和reduce方法
    job.setMapperClass(TransitiveClosureMapperImpl.class);
    job.setReducerClass(TransitiveClosureReducerImpl.class);

    // 设置map方法的输出键值对数据类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // 设置reduce方法的输出键值对数据类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
