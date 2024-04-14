package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureMapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class TransitiveClosureMapperImpl extends TransitiveClosureMapper{
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException
  {
    
  }
}
