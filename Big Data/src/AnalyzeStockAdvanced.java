import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AnalyzeStockAdvanced {

	private static final int TICKER = 0;
	private static final int DATE = 1;
	private static final int OPEN = 2;
	private static final int HIGH = 3;
	private static final int LOW = 4;

	public static class ChainJobs extends Configured implements Tool {

		private static final String INTERMEDIATE_PATH = "intermediate_output";

		@Override
		public int run(String[] args) throws Exception {
			
			Configuration conf1 = new Configuration();
			Job job1 = Job.getInstance(conf1, "Job1");
			job1.setJarByClass(AnalyzeStockAdvanced.class);
			job1.setMapperClass(Mapper1.class);
			// job1.setCombinerClass(StockCombiner.class);
			job1.setReducerClass(Reducer1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setNumReduceTasks(20);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(INTERMEDIATE_PATH));
			job1.waitForCompletion(true);

			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf2, "Job2");
			job2.setJarByClass(AnalyzeStockAdvanced.class);
			job2.setMapperClass(Mapper2.class);
			// job2.setCombinerClass(StockCombiner.class);
			job2.setReducerClass(Reducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setNumReduceTasks(20);
			FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE_PATH));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));

			return job2.waitForCompletion(true) ? 0 : 1;
		}

	}

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Text mapKey = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd");

			String[] lines = value.toString().split("\n");
			for(int i=0; i<lines.length; i++) {
				
				String[] row = lines[i].split(",");
				Date date = null;
				try {
					date = format.parse(row[DATE]);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				if (date == null) {
					// The data in input file is corrupted for this row. Ignore this
					// row.
					return;
				}
				String name = row[TICKER];
				String low = row[LOW];
				String high = row[HIGH];
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				int yearVal = cal.get(Calendar.YEAR);
				String yearStr = Integer.toString(yearVal);
				String keyString = yearStr  + "|" + name; 
				mapKey.set(keyString);
				String valString = high + "," + low;
				context.write(mapKey, new Text(valString));

			}
			
		}

	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {


		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String keyStr = key.toString();
			String[] yearName = new String[2];
			StringTokenizer itr = new StringTokenizer(keyStr, "|");
			int counter = 0;
			while (itr.hasMoreTokens()) {
				yearName[counter] = itr.nextToken();
				counter ++;
		      }
			String yearKey = yearName[0];
			String tickerName = yearName[1];
			System.out.println(yearKey + "," + tickerName);

			float lowest = Float.MAX_VALUE;
			float highest = Float.MIN_VALUE;
			for (Text txt : values) {
				
				String[] row = txt.toString().split(",");
				float high = 0;
				float low = 0;
				try{
					high = Float.parseFloat(row[0]);
				} catch(NumberFormatException ex) {
					
				}
				try{
					low = Float.parseFloat(row[1]);
				} catch(NumberFormatException ex) {
					
				}
				
				if (row.length != 2) {
					// One of the fields (low or high) is missing in the input
					// file. Ignore this date value.
					continue;
				}
				
				if(high > highest)
					highest = high;
				
				if(low < lowest)
					lowest = low;
				
			}

			String details = tickerName + "," + highest + "," + lowest;
			context.write(new Text(yearKey), new Text(details));
		}
	}
	
	
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

		private Text mapKey = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] lines = value.toString().split("\n");
			for(int i=0; i<lines.length; i++) {
				
				StringTokenizer itr = new StringTokenizer(lines[i], "\t");
				String[] splits = new String[2];
				int counter = 0;
				while (itr.hasMoreTokens()) {
					splits[counter] = itr.nextToken();
					counter ++;
			      }
				System.out.println(splits[0]+"---" + splits[1] );
				mapKey.set(splits[0]);
				context.write(mapKey, new Text(splits[1]));

			}
			
		}

	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {


		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			
			float highest = Float.MIN_VALUE;
			String bestTicker = "Ticker";
			float bestHigh = 0;
			float bestLow = 0;
			for (Text txt : values) {
				
				String[] row = txt.toString().split(",");
				String name = row[0];
				float high = Float.parseFloat(row[1]);
				float low = Float.parseFloat(row[2]);
				float diff = high - low;
				if(diff>highest) {
					bestHigh = high;
					bestLow = low;
					bestTicker = name;
				}
				
			}

			String details = bestTicker + "," + bestHigh + "," + bestLow + "," + (bestHigh-bestLow);
			context.write(key, new Text(details));
		}
	}
	
	

	public static void main(String[] args) throws Exception {

		ToolRunner.run(new Configuration(), new ChainJobs(), args);
	}
}