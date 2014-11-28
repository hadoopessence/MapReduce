/**
 * 
 */
package org.apache.hadoopessence.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CustomerInfoReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable customerId,Iterable<Text> values,Context context) 
			throws IOException, InterruptedException{
		System.out.println("Reducer : Key : "+customerId.toString());
		context.write(customerId, values.iterator().next());
	}
}
