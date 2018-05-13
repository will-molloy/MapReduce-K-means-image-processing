package hadoop.process_units;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ProcessUnits {

    /**
     * input and output directories in resources dir.
     * Build jar and run:
     * hadoop jar hadoop.jar hadoop.process_units.ProcessUnits input_dir output_dir
     */
    public static void main(String... args) throws Exception {
        JobConf conf = new JobConf(ProcessUnits.class);

        conf.setJobName("avg_electricity_units");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
        conf.setMapperClass(UnitsMapper.class);
        conf.setCombinerClass(UnitsReducer.class);
        conf.setReducerClass(UnitsReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileSystem.get(conf).delete(new Path(args[1]), true); // delete output_dir for us
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}