package org.techmytalk.dedupe;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * The Class SampleMapReduce.
 */
public class DedupeMR {
	private static IntWritable cnt = new IntWritable(1);
	private static KeyGroup keygrp = new KeyGroup();

	public static class DedupeMapper extends
			Mapper<LongWritable, Text, KeyGroup, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();

			keygrp.text = line;
			keygrp.value = 1;
			context.write(keygrp, cnt);
		}
	}

	public static class DedupeReducer extends
			Reducer<KeyGroup, IntWritable, KeyGroup, IntWritable> {
		KeyGroup keyGroup = new KeyGroup();

		public void reduce(KeyGroup key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable inte : values) {
				sum += inte.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class KeyGroup implements WritableComparable<KeyGroup> {

		public String text;
		public Integer value;

		public void write(DataOutput out) throws IOException {

			WritableUtils.writeString(out, text);
			out.writeInt(value);
		}

		public void readFields(DataInput in) throws IOException {
			this.text = WritableUtils.readString(in);
			this.value = in.readInt();

		}

		public int compareTo(KeyGroup o) {

			int cnt = this.value.compareTo(o.value);
			return cnt == 0 ? this.text.compareTo(o.text) : cnt;

		}

		@Override
		public String toString() {
			return this.text.toString();
			// return super.toString();
		}

	}

	public class DedupeJob extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			FileUtils.deleteDirectory(new File("/Local/data/output"));
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "DedupeMR");
			job.setJarByClass(DedupeMR.class);
			job.setNumReduceTasks(2);
			job.setMapperClass(DedupeMapper.class);
			job.setReducerClass(DedupeReducer.class);

			job.setOutputKeyClass(KeyGroup.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(
					"/Local/data/sampleword.csv"));
			FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			return 0;
		}

	}

	public static void main(String[] args) throws Exception {

		Tool tool = new DedupeMR().new DedupeJob();
		// FileUtils.deleteDirectory(new File("/Local/data/output"));

		tool.run(null);
	}

}