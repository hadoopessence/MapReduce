package org.hadoopessence;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 
 * 
 * Combiner
 * 
 * @author nitin
 *
 */
public class AverageMR {

	private static class AverageMapper extends
			Mapper<LongWritable, Text, IntWritable, LongWritable> {

		IntWritable cnt=new IntWritable(1);
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (value != null) {
				System.out.println("===>" + key.toString());
				String line = value.toString();
				String[] linesplit = line.split(",");
				Long lngV = Long.parseLong(linesplit[1]);
				
				context.write(cnt, new LongWritable(lngV));

			}

		}

	}

	private static class AverageCombiner extends
			Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {

		
		
		public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long sum=0;
			int cnt=0;
			for (LongWritable longWritable : values) {
				sum=sum+longWritable.get();
				cnt++;
			}
			long average=sum/cnt;
			context.write(new IntWritable(1), new LongWritable(average));
		}

	}
	
	private static class AverageReducer extends Reducer<IntWritable,LongWritable,IntWritable,LongWritable>{
		
		public void reduce(IntWritable key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
		long sum=0;
		int cnt=0;
		for (LongWritable longWritable : values) {
			sum=sum+longWritable.get();
			cnt++;
		}
		long average=sum/cnt;
		context.write(new IntWritable(cnt), new LongWritable(average));
	}
	}
		
		
	

	public class AverageMapperJob extends Configured implements Tool {

		public int run(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "AverageMapper");
			job.setMapperClass(AverageMapper.class);
			job.setCombinerClass(AverageCombiner.class);
			

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setNumReduceTasks(1);
			// FileInputFormat.setMaxInputSplitSize(job, 12);
			FileInputFormat.addInputPath(job, new Path(
					"/Local/data/Employee.csv"));
			FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			return 0;

		}

	}

	public static void main(String[] args) throws Exception {
		FileUtils.deleteDirectory(new File("/Local/data/output"));
		Tool tool = new AverageMR().new AverageMapperJob();

		tool.run(null);
	}

}
