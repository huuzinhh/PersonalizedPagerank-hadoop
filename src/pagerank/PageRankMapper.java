package pagerank;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String[] parts = value.toString().split("\\s+");
        if (parts.length < 2) return;

        String page = parts[0];
        double rank = Double.parseDouble(parts[1]);
        String[] links = parts.length > 2 ? parts[2].split(",") : new String[0];

        for (String link : links) {
            context.write(new Text(link), new Text("RANK:" + (rank / links.length)));
        }

        context.write(new Text(page), new Text("LINKS:" + (parts.length > 2 ? parts[2] : "")));
    }
}

