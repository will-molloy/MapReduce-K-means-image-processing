package nz.ac.auckland.mapreduce.hadoop;

import com.google.gson.JsonParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

import static java.util.Arrays.stream;

public class RedditCommentsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Logger log = Logger.getLogger(RedditCommentsMapper.class);

    /**
     * Process chunks of JSON comment data and create key value pairs: [subreddit name, score]
     * Can optionally filter the data, e.g. is gilded comment.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) {
        stream(value.toString()
                .split("\n"))
                .map(string -> new JsonParser().parse(string).getAsJsonObject())
                .forEach(comment -> {
                    try {
                        context.write(
                                new Text(comment.get("subreddit").getAsString().toLowerCase()),
                                new IntWritable(comment.get("score").getAsInt()));
                    } catch (IOException | InterruptedException e) {
                        log.error("Mapper failed.", e);
                    }
                });
    }

}
