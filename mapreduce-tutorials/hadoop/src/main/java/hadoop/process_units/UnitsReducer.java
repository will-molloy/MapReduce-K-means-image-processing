package hadoop.process_units;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class UnitsReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key,
                       Iterator<DoubleWritable> values, // Input from Mapper (must match Mapper output)
                       OutputCollector<Text, DoubleWritable> output, // Output to output_dir
                       Reporter reporter) throws IOException {

        while (values.hasNext()) {
            output.collect(key, new DoubleWritable(values.next().get()));
        }
    }
}