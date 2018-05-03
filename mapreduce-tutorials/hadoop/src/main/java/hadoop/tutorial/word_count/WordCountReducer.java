package hadoop.tutorial.word_count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(word,
                new IntWritable(StreamSupport.stream(values.spliterator(), false)
                        .mapToInt(IntWritable::get)
                        .sum()));
    }

}