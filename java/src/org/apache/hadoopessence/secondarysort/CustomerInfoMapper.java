/**
 * 
 */
package org.apache.hadoopessence.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CustomerInfoMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String customerRecord = value.toString();
		String [] recordContent = customerRecord.split(",");
		System.out.println("Mapper Key : "+recordContent[3]);
		System.out.println("Mapper value : "+customerRecord);
		context.write(new IntWritable(Integer.parseInt(recordContent[3])), new Text(customerRecord));
	}
}
