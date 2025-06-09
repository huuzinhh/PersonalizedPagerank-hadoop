package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final double DAMPING = 0.85;
    private static final String SOURCE_PAGE = "P1"; // Trang được cá nhân hóa

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        double sumRank = 0.0;
        String links = "";

        for (Text val : values) {
            String content = val.toString();
            if (content.startsWith("RANK:")) {
                sumRank += Double.parseDouble(content.substring(5));
            } else if (content.startsWith("LINKS:")) {
                links = content.substring(6);
            }
        }

        double personalizedJump = key.toString().equals(SOURCE_PAGE) ? (1 - DAMPING) : 0.0;
        double newRank = personalizedJump + DAMPING * sumRank;

        context.write(key, new Text(newRank + (links.isEmpty() ? "" : "\t" + links)));
    }
}

