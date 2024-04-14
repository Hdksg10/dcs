package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DifferenceReducerImpl extends DifferenceReducer{
  private static final String RNAME = "R";
  private static final String SNAME = "S";
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException
  {
    Map<String, Boolean> RMap = new HashMap<>();
    Map<String, Boolean> SMap = new HashMap<>();
    for (Text value : values) {
      String[] split = value.toString().split(" ");
      String name = split[0];
      String id = split[1];
      if (name.equals(RNAME)) {
        RMap.put(id, true);
      } else if (name.equals(SNAME)) {
        SMap.put(id, true);
      }
    }
    for (String id : RMap.keySet()) {
      if (!SMap.containsKey(id)) {
        String newKey = key.toString() + "\t" + id;
        context.write(new Text(newKey), NullWritable.get());
      }
    }
  }
}