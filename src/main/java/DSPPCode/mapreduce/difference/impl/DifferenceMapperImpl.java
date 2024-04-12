package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.lib.input.FileSplit;
import java.io.IOException;

public class DifferenceMapperImpl extends DifferenceMapper {
    private static final String RNAME = "R";
    private static final String SNAME = "S";
    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException
    {
      boolean isS = false;
      // Check File Name R os S
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      if (fileName.contains(RNAME)) {
        isS = false;
      } else if (fileName.contains(SNAME)) {
        isS = true;
      }
      // Split the input line
      String[] line = value.toString().split("\t");
      // Output the key-value pair
      if (isS) {
        context.write(new Text(line[0]), new Text(SNAME + " " + line[1]));
      } else {
        context.write(new Text(line[0]), new Text(RNAME + " " + line[1]));
      }
    }
}
