package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureMapper;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class TransitiveClosureMapperImpl extends TransitiveClosureMapper{
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException
  {
    String line = value.toString();
    System.out.println("line: " + line);
    String[] split = line.split("\t");
    String member1 = split[0];
    String member2 = split[1];
    System.out.println("member1: " + member1 + " member2: " + member2);
    context.write(new Text(member1), new Text("0" + "," + member2));
    context.write(new Text(member2), new Text("1" + "," + member1));
  }
}
