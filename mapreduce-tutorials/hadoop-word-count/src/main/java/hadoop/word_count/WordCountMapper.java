package hadoop.word_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) {
        Arrays.stream(value.toString()
                .split(" "))
                .map(s -> s.replaceAll("[\\W]|_", "").toUpperCase().trim())
                .forEach(word -> {
                    try {
                        context.write(new Text(word), new IntWritable(1));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }

}
