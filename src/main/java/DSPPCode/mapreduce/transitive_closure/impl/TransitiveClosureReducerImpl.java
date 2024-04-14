package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;
import java.util.ArrayList;

public class TransitiveClosureReducerImpl extends TransitiveClosureReducer {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException
  {
    // Header
    // String header = context.getConfiguration().get("header");

    ArrayList<String> list0 = new ArrayList<String>();
    ArrayList<String> list1 = new ArrayList<String>();
    // System.out.println("Reduce" + " " + key.toString());
    for (Text value : values) {
      String[] valueArray = value.toString().split(",");
      if (valueArray[0].equals("0")) {
        list0.add(valueArray[1]);
      } else {
        list1.add(valueArray[1]);
      }
    }

    for (String value0 : list0) {
      for (String value1 : list1) {
        context.write(new Text(value1), new Text(value0));
      }
    }
  }
}
