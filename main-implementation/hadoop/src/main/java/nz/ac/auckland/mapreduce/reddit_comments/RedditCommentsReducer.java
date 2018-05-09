package nz.ac.auckland.mapreduce.reddit_comments;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.function.Function;

import static java.util.stream.StreamSupport.stream;

public class RedditCommentsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    /**
     * Reduce votes per set of sub reddit comments by calculating the mean.
     */
    @Override
    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        ImmutableList<Integer> list = toList.apply(values);
        double mean = list.stream()
                .mapToDouble(Integer::doubleValue)
                .sum()
                / list.size();
        context.write(word, new DoubleWritable(mean));
    }

    private Function<Iterable<IntWritable>, ImmutableList<Integer>> toList = (iterable) ->
            ImmutableList.copyOf(stream(iterable.spliterator(), false)
                    .mapToInt(IntWritable::get)
                    .iterator());

}
