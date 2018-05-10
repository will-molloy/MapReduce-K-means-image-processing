package nz.ac.auckland.mapreduce.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class RedditCommentsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    /**
     * Reduce votes per set of sub reddit comments by calculating the mean.
     */
    @Override
    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double mean = StreamSupport.stream(values.spliterator(), false)
                .mapToDouble(IntWritable::get)
                .average()
                .orElse(0);
        context.write(word, new DoubleWritable(mean));
    }

}
