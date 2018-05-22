package kmeans.util;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;

public class ArgParserFactory {

    public static ArgumentParser get() {
        ArgumentParser argumentParser = ArgumentParsers.newFor("scala-spark-kmeans")
                .build()
                .defaultHelp(true)
                .description("KMeans image processing in parallel");
        argumentParser.usage("kmeans.Main pointsFile outFile numClusters repeats/frames " +
                "[-h] [-p {mapreduce,imagesplit,sequential}] [-s {random,kmeans++,parallelkmeans++}]\n");

        argumentParser.addArgument("pointsFile")
                .required(true)
                .help("Input image/video file name");
        argumentParser.addArgument("outFile")
                .required(true)
                .help("Output file name");
        argumentParser.addArgument("numClusters")
                .required(true)
                .type(Integer.class)
                .help("Number of clusters per image");
        argumentParser.addArgument("repeats/frames")
                .required(true)
                .type(Integer.class)
                .help("Number of image repeats or max video frames");
        argumentParser.addArgument("-p", "--parallel")
                .choices("mapreduce", "imagesplit", "sequential")
                .setDefault("sequential")
                .help("Use MapReduce implementation (pixel partitioning) or Process images in parallel (image partitioning)");
        argumentParser.addArgument("-s", "--seeder")
                .choices("random", "kmeans++", "parallelkmeans++")
                .setDefault("random")
                .help("KMeans seeder, KMeans++ produces a better result\n" +
                        "Will default to kmeans++ if parallelkmeans++ is specified with imagesplit");
        return argumentParser;
    }
}
