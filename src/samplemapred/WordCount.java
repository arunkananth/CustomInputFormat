package samplemapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import samplemapred.CustomFileInputFormat;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    // A constant value of one, to count each word
    private final static IntWritable one = new IntWritable(1);
    // A Text object to store each word
    private Text word = new Text();

    // The map method processes each line of input
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Tokenize the input line
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        // Set the word to the Text object
        word.set(itr.nextToken());
        // Write the word and count (1) to the context
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // A variable to store the sum of counts for each word
    private IntWritable result = new IntWritable();

    // The reduce method sums up counts for each word
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        // Sum the counts for each word
        sum += val.get();
      }
      // Set the sum to the result
      result.set(sum);
      // Write the word and its total count to the context
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Path inPath = null;
    Path outPath = null;

    // Create a configuration object
    Configuration conf = new Configuration();
    // Create a job object with the configuration and job name
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Check if input and output paths are provided
    if (args.length == 0) {
      System.out.println("###########**********Input output file not provided, using defaults..!");
      inPath = new Path("Data/input.txt");
      outPath = new Path("Data/Output.txt");
    } else {
      inPath = new Path(args[0]);
      outPath = new Path(args[1]);
    }

    // Set the input and output paths for the job
    FileInputFormat.addInputPath(job, inPath);
    job.setInputFormatClass(CustomFileInputFormat.class);
    FileSystem fs = FileSystem.get(new Configuration());
    System.out.println(fs.getConf().toString());
    // Delete the output path if it already exists
    fs.delete(outPath, true);
    FileOutputFormat.setOutputPath(job, outPath);

    // Wait for the job to complete and exit
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
