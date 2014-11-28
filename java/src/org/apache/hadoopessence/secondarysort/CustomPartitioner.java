/**
 * 
 */
package org.apache.hadoopessence.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

 class CustomPartitioner extends HashPartitioner<IntWritable,Text> {

	@Override
	public int getPartition(IntWritable key, Text value, int numOfReducers) {
		// TODO Auto-generated method stub
		System.out.println("num of reducers : "+numOfReducers);
		int partition = key.hashCode() & Integer.MAX_VALUE;
		System.out.println("================"+partition/numOfReducers);
		return partition/numOfReducers;
	}
}
