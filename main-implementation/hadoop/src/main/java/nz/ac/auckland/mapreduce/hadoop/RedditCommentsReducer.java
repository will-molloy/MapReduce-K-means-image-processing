package nz.ac.auckland.mapreduce.hadoop;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RedditCommentsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    /**
     * Reduce votes per set of sub reddit comments by calculating the mean.
     */
    @Override
    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        ImmutableList<IntWritable> list = ImmutableList.copyOf(values);
        double mean = list.stream()
                .mapToDouble(v -> (double)v.get())
                .sum()
                / (list.size() * 1.0);
        context.write(word, new DoubleWritable(mean));
    }

}
