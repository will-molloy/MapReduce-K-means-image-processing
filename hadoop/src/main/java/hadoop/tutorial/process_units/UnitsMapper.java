package hadoop.tutorial.process_units;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

import static java.lang.Integer.parseInt;

public class UnitsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key,
                    Text value, // input file
                    OutputCollector<Text, DoubleWritable> output, // Outputs (i.e. inputs to Reducer)
                    Reporter reporter)
            throws IOException {
        String line = value.toString();
        StringTokenizer tokens = new StringTokenizer(line, "\t");
        String year = tokens.nextToken();

        double meanPrice = 0;
        double total = tokens.countTokens();
        while (tokens.hasMoreTokens()) {
            meanPrice += parseInt(tokens.nextToken().trim());
        }

        meanPrice /= total;
        output.collect(new Text(year), new DoubleWritable(meanPrice));
    }
}