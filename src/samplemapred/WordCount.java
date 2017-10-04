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

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	Path inPath = null;
	Path outPath = null;
	    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    //FileOutputFormat.setOutputPath(job, new Path(args[1]));

    if(args.length == 0)
    {
        System.out.println("###########**********Input output file not provided, using defaults..!");
        inPath = new Path("Data/input.txt");
        outPath = new Path("Data/Output.txt");
    }
    else
    {
    	inPath = new Path(args[0]);
        outPath = new Path(args[1]);
    }
    
    FileInputFormat.addInputPath(job, inPath);
    job.setInputFormatClass(CustomFileInputFormat.class);
    FileSystem fs = FileSystem.get(new Configuration());
    System.out.println(fs.getConf().toString());
 // true stands for recursively deleting the folder you gave
    fs.delete(outPath, true);
    FileOutputFormat.setOutputPath(job, outPath);
    //System.out.println(job.toString());
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
}