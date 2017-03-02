import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCount {
	

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ".,{}[]()\t ");
      boolean caseSensitive = true;
      caseSensitive = Boolean.parseBoolean(context.getConfiguration().get("CASESENSITIVE"));    	
      while (itr.hasMoreTokens()) {
    	  String w = itr.nextToken();
    	  if(!caseSensitive)
    		 w = w.toLowerCase();
        word.set(w);
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
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MyWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    if(args.length != 4) {
    	System.out.println("Invalid Number of Arguments!");
    	return;
    }
    int reducerTaskCount = 2;
    try{
    	reducerTaskCount = Integer.parseInt(args[0]);
    } catch(NumberFormatException ex) {
    	System.out.println("Invalid reducer count set!");
    	return;
    }
    try{
      	Boolean.parseBoolean(args[1]);    	
      } catch(Exception ex) {
      	System.out.println("Invalid Case Sensitive argument set!");
      	return;
      }
    
    conf.set("CASESENSITIVE", args[1]);
    job.setNumReduceTasks(reducerTaskCount);
    String fileNames = args[2];
    String[] allFiles = fileNames.split(",");
    for(int i=0; i<allFiles.length; i++) {
    	MultipleInputs.addInputPath(job,new Path(allFiles[i]), TextInputFormat.class, TokenizerMapper.class);
    }
    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}