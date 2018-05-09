package nz.ac.auckland.mapreduce.reddit_comments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


public class Main {

    private static final Logger log = Logger.getLogger(Main.class);

    /**
     * Run:
     * hadoop jar hadoop.jar nz.ac.auckland.mapreduce.reddit_comments.Main input_dir output_dir
     */
    public static void main(String... args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration configuration = new Configuration();
        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = new Job(configuration, "se751_group9_hadoop");
        job.setJarByClass(Main.class);
        job.setMapperClass(RedditCommentsMapper.class);
        job.setReducerClass(RedditCommentsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem.get(configuration).delete(new Path(args[1]), true); // delete output_dir for us
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        int exit = job.waitForCompletion(true) ? 0 : 1;
        log.info(String.format("TIME: %d (ms)", (System.currentTimeMillis() - start)));
        System.exit(exit);
    }
}
