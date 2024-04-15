package DSPPCode.mapreduce.common_pagerank.impl;
import DSPPCode.mapreduce.common_pagerank.question.PageRankReducer;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer{
  private static final String TOTAL_PAGE = "1";
  private static final String GROUP_NAME = "page_rank";
  private static final String COUNTER_NAME = "converge_page_count";
  public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
      throws IOException, InterruptedException
  {
    int totalPage = context.getConfiguration().getInt(TOTAL_PAGE, 0);

    double sum = 0;
    ReducePageRankWritable pageInfo = null;
    String info = null;
    for (ReducePageRankWritable value : values) {
      String tag = value.getTag();
      if (tag.equals(ReducePageRankWritable.PR_L)) {
        double rank = Double.parseDouble(value.getData());
        // System.out.println(rank);
        sum += rank;
      } else if (tag.equals(ReducePageRankWritable.PAGE_INFO)){
        // System.out.println("tag: " + tag);
        // System.out.println(value.getData());
        pageInfo = value;
        info = value.getData();
      }
    }
    // System.out.println(key.toString());
    if (pageInfo != null) {
      double rank = 0.15 / totalPage + 0.85 * sum; // decay factor = 0.85
       // info = pageInfo.getData();
      // System.out.println(info);
      String[] infos = info.split("\\s+");
      double lastRank = Double.parseDouble(infos[1]);
      if (Math.abs(rank - lastRank) < 1e-6) {
        context.getCounter(GROUP_NAME, COUNTER_NAME).increment(1);
      }
      infos[1] = String.valueOf(rank);
      StringBuilder sb = new StringBuilder();
      for (String s : infos) {
        sb.append(s).append(" ");
      }
      // System.out.println(sb.toString().trim());
      context.write(new Text(sb.toString().trim()), null);
    }
  }
}
