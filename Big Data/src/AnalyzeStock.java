import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeStock {

	private static final String AGGREGATOR = "aggregator";
	private static final String START_DATE = "start_date";
	private static final String END_DATE = "end_date";
	private static final String COLUMN = "column";
	private static final int TICKER = 0;
	private static final int DATE = 1;
	private static final int OPEN = 2;
	private static final int HIGH = 3;
	private static final int LOW = 4;
	private static final int CLOSE = 5;
	private static final int VOLUME = 6;
	private static final int EX_DIVIDEND = 7;
	private static final int SPLIT_RATIO = 8;
	private static final int ADJ_OPEN = 9;
	private static final int ADJ_HIGH = 10;
	private static final int ADJ_LOW = 11;
	private static final int ADJ_CLOSE = 12;
	private static final int ADJ_VOLUME = 13;

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		private Text ticker = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			DateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy");
			DateFormat fileFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date startDate = null;
			Date endDate = null;
			String column = null;
			try {
				startDate = inputFormat.parse(conf.get(START_DATE));
				endDate = inputFormat.parse(conf.get(END_DATE));
				column = conf.get(COLUMN);
			} catch (ParseException e) {
				e.printStackTrace();
			}

			String[] lines = value.toString().split("\n");
			for (int i = 0; i < lines.length; i++) {

				String[] row = lines[i].split(",");
				Date date = null;
				try {
					date = fileFormat.parse(row[DATE]);
				} catch (ParseException e) {
					e.printStackTrace();
					return;
				}
				if (date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0) {
					String name = row[TICKER];
					float stockVal = 0.0f;
					FloatWritable val = new FloatWritable((float) 0.0);
					if (column != null && column.equals("low")) {
						try {
							stockVal = Float.parseFloat(row[LOW]);
						} catch (NumberFormatException ex) {
						}
						val.set(stockVal);
					} else if (column != null && column.equals("close")) {
						try {
							stockVal = Float.parseFloat(row[CLOSE]);
							if(stockVal == 159.0) {
								System.out.println("hit");
							}
						} catch (NumberFormatException ex) {
						}
						val.set(stockVal);
					} else if (column != null && column.equals("high")) {
						try {
							stockVal = Float.parseFloat(row[HIGH]);
						} catch (NumberFormatException ex) {
						}
						val.set(stockVal);
					}
					ticker.set(name);
					context.write(ticker, val);

				}

			}
		}
	}

	public static class Aggregator extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String aggr = conf.get(AGGREGATOR);
			if (aggr != null && aggr.equals("avg")) {

				float sum = 0;
				int count = 0;
				for (FloatWritable val : values) {
					sum += val.get();
					count++;
				}
				float avg = 0;
				if (count != 0)
					avg = sum / count;
				result.set(avg);

			} else if (aggr != null && aggr.equals("min")) {
				float min = Float.MAX_VALUE;
				for (FloatWritable val : values) {
					if (val.get() < min) {
						min = val.get();
					}
				}
				result.set(min);
			} else if (aggr != null && aggr.equals("max")) {
				float max = Float.MIN_VALUE;
				for (FloatWritable val : values) {
					if (val.get() > max) {
						max = val.get();
					}
				}
				result.set(max);
			}
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[5]))) {
			fileSystem.delete(new Path(args[5]));
		}
		conf.set(START_DATE, args[0]);
		conf.set(END_DATE, args[1]);
		conf.set(AGGREGATOR, args[2]);
		conf.set(COLUMN, args[3]);
		Job job = Job.getInstance(conf, "Analyze Stock");
		job.setJarByClass(AnalyzeStock.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(Aggregator.class);
		job.setReducerClass(Aggregator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}