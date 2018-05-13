package hadoop.process_units;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.stream;

public class UnitsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key,
                    Text value, // input file
                    OutputCollector<Text, DoubleWritable> output, // Outputs (i.e. inputs to Reducer)
                    Reporter reporter)
            throws IOException {
        String[] tokens = value.toString().split("\t");
        String year = tokens[0];

        double total = tokens.length - 1;
        double mean = stream(copyOfRange(tokens, 1, tokens.length))
                .map(v -> v.replaceAll("[^0-9]", ""))
                .mapToInt(Integer::parseInt)
                .sum()
                / total;

        output.collect(new Text(year), new DoubleWritable(mean));
    }
}