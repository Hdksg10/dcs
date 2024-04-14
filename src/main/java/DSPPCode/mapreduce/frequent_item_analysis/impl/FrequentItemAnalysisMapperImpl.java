package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class FrequentItemAnalysisMapperImpl extends FrequentItemAnalysisMapper{
  public void map(LongWritable key,Text value,Context context)
      throws IOException, InterruptedException
  {

    String line = value.toString();
    String[] items = line.split(",");
    // System.out.println(items[0]);
    List<String> itemsList = new ArrayList<>();

    for (String item : items) {
      itemsList.add(item);
    }
    SortHelperImpl sortHelper = new SortHelperImpl();
    List<String> sortedItems = sortHelper.sortSeq(itemsList);
    items = sortedItems.toArray(new String[0]);
    // System.out.println("items: " + items[0] + " " + items[1]);
    int n = context.getConfiguration().getInt("number.of.pairs", 0);
    List<List<String>> subsets = getSubsets(items, n);
    for (List<String> subset : subsets) {
      StringBuilder sb = new StringBuilder();
      for (String item : subset) {
        sb.append(item).append(" ");
      }
      // System.out.println(sb.toString().trim());
      context.write(new Text(sb.toString().trim()), new IntWritable(1));
    }
  }

  private static List<List<String>> getSubsets(String[] items, int n) {
    List<List<String>> result = new ArrayList<>();
    backtrack(items, 0, new ArrayList<>(), result, n);
    return result;
  }

  private static void backtrack(String[] items, int start, List<String> subset, List<List<String>> result, int n) {
    if (subset.size() == n) {
      result.add(new ArrayList<>(subset));
      return;
    }

    for (int i = start; i < items.length; i++) {
      subset.add(items[i]);
      backtrack(items, i + 1, subset, result, n);
      subset.remove(subset.size() - 1);
    }
  }

}
