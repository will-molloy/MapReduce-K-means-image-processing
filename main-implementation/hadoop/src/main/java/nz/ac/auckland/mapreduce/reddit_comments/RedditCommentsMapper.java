package nz.ac.auckland.mapreduce.reddit_comments;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import nz.ac.auckland.mapreduce.reddit_comments.model.Comment;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.function.Function;

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
                .map(toRedditComment)
                .forEach(comment -> {
                    try {
                        context.write(new Text(comment.getSubReddit()),
                                new IntWritable(comment.getScore()));
                    } catch (IOException | InterruptedException e) {
                        log.error("Mapper failed.", e);
                    }
                });
    }

    private Function<String, Comment> toRedditComment = (jsonLine) -> {
        JsonParser parser = new JsonParser();
        JsonObject commentData = parser.parse(jsonLine).getAsJsonObject();
        return new Comment(
                commentData.get("score").getAsInt(),
                commentData.get("subreddit").getAsString(),
                commentData.get("gilded").getAsInt());
    };

}
