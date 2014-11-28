/**
 * 
 */
package org.apache.hadoopessence.secondarysort;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CustomerSortingDriver extends Configured implements Tool{

	/**
	 * @param args
	 */
	
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int i = ToolRunner.run(new CustomerSortingDriver(), args);
		System.out.println(i);
	}
	
	public int run(String[] paramArrayOfString) throws Exception {
		FileUtils.deleteDirectory(new File("/Local/data/output"));
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();;
		Job job = Job.getInstance(conf,"CustomerSortingDriver");
		job.setJarByClass(getClass());
		
		job.setMapperClass(CustomerInfoMapper.class);
		job.setReducerClass(CustomerInfoReducer.class);
		
		job.setPartitionerClass(CustomPartitioner.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(12);
		
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(
				"/Local/data/CustInfo"));
		FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
		
		
		//FileInputFormat.setInputPaths(job,new Path(paramArrayOfString[0]));
		//FileOutputFormat.setOutputPath(job,new Path(paramArrayOfString[1]));
		
		return job.waitForCompletion(true)?0:1;
	}
}
