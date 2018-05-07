package nz.ac.auckland.mapreduce.reddit_comments;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import nz.ac.auckland.mapreduce.reddit_comments.model.Comment;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class RedditCommentsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) {
        Arrays.stream(value.toString()
                .split("\n"))
                .map(this::toRedditComment)
                .forEach(comment -> {
                    try {
                        context.write(new Text(comment.getSubReddit()), new IntWritable(comment.getUps()));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }

    private Comment toRedditComment(String jsonLine) {
        JsonParser parser = new JsonParser();
        JsonObject commentObj = parser.parse(jsonLine).getAsJsonObject();

        JsonElement ups = commentObj.get("ups");
        JsonElement subReddit = commentObj.get("subreddit");

        return new Comment(ups.getAsInt(), subReddit.getAsString());
    }

}
