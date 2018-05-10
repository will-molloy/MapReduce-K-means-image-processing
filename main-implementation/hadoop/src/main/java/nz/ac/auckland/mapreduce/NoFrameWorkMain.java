package nz.ac.auckland.mapreduce;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class NoFrameWorkMain {

    public static void main(String... args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration configuration = new Configuration();
        String[] files = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        FileSystem.get(configuration).delete(new Path(args[1]), true); // delete output_dir for us

        System.out.println("MAPPING");
        ImmutableMap<String, ImmutableList<Integer>> intermediatePairs = parseAndMap(input);
        System.out.println("REDUCING");
        ImmutableList<String> resultPairs = reduce(intermediatePairs);
        System.out.println("WRITING RESULT");
        writeToOutput(resultPairs, output);

        System.out.println(String.format("TIME: %d (ms)", (System.currentTimeMillis() - start)));
    }

    private static ImmutableMap<String, ImmutableList<Integer>> parseAndMap(Path input) throws FileNotFoundException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(String.format("%s/%s", input.getParent(), input.getName())));
        Map<String, List<Integer>> map = new HashMap<>();
        bufferedReader
                .lines()
                .map(string -> new JsonParser().parse(string).getAsJsonObject())
                .forEach(jsonObject -> {
                    String subreddit = jsonObject.get("subreddit").getAsString().toLowerCase();
                    List<Integer> list = map.containsKey(subreddit) ? map.get(subreddit) : new ArrayList<>();
                    map.put(subreddit, list);
                    list.add(jsonObject.get("score").getAsInt());
                });
        return map
                .entrySet()
                .stream()
                .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey,
                        e -> ImmutableList.copyOf(e.getValue())), ImmutableMap::copyOf));
    }

    private static ImmutableList<String> reduce(ImmutableMap<String, ImmutableList<Integer>> intermediatePairs) {
        return intermediatePairs
                .entrySet()
                .stream()
                .map(e -> String.format("%s\t%s", e.getKey(), mean(e.getValue())))
                .sorted(String::compareTo)
                .collect(Collectors.collectingAndThen(Collectors.toList(), ImmutableList::copyOf));
    }

    private static double mean(ImmutableList<Integer> list) {
        return list.stream()
                .mapToDouble(Integer::doubleValue)
                .sum()
                / list.size();
    }

    private static void writeToOutput(ImmutableList<String> resultPairs, Path output) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(String.format("%s/%s", output.getParent(), output.getName())));
        resultPairs.forEach(result -> {
            try {
                bufferedWriter.write(result);
                bufferedWriter.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        bufferedWriter.close();
    }

}
