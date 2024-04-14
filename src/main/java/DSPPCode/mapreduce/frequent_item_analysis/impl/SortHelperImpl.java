package DSPPCode.mapreduce.frequent_item_analysis.impl;


import java.util.List;
import DSPPCode.mapreduce.frequent_item_analysis.question.SortHelper;
import java.util.Collections;
public class SortHelperImpl extends SortHelper{
  @Override
  public List<String> sortSeq(List<String> input)
  {
    Collections.sort(input);
    return input;
  }
}
