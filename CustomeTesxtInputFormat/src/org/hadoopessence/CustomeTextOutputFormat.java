

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
/**
 * 
 * The OutputFormat defined the where the data could be persisted after processing MapReduce job. 
 * Hadoop provides various classes and interfaces to accomplish different type for format e.g. 
 * SequenceFileOutputFormat to compress the binary key-value output, TextOutputFormat to write into tab 
 * delimited new line etc.
 * OutputFormat uses RecordWriter to searlized data and writing to location in HDFS. 
 * RecordWriter uses write operation to write key-values to disk whereas close operation closes this RecordWriter.
 * 
 * 
 * This MapReduce program take the input as key-value and write the output on value sorted based.
 * We know the output from mapreduce will be sorted on Key either natural sorting or custom sorting. Here question arises 
 * how to short based on values.  
 * 
 * The easy way is to swap the key-value and but here my aim is to explain how to customize the output 
 * format without swapping key-value.
 * 
 * So here we have customize MultipleTextOutputFormat to customize the out which is going to write on the disk and change 
 * the file name.
 * 
 * 
 * 
 * MultipleTextOutputFormat extends the MultipleOutputFormat, allowing to write the output
 * data to different output files in Text output format.
 * 
 * This class extends the MultipleOutputFormat, allowing to write the output data 
 * to different output files in sequence file output format. 
 * 
 * @author nitin
 *
 */

public class CustomeTextOutputFormat {

	public static class SortMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {
		private Text text = new Text();
		private IntWritable intW = new IntWritable();

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] splitValue;
			if (line != null && !line.equalsIgnoreCase("")) {
				splitValue = line.split(",");
				text.set(line);
				intW.set(Integer.parseInt(splitValue[1]));
				output.collect(intW, text);
			}

		}
	}

	private static class NaturalKeyGroupingComparator extends
			WritableComparator {

		/**
		 * Constructor.
		 */
		protected NaturalKeyGroupingComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return -1 * a.compareTo(b);

		}

	}

	public static void main(String[] args) throws Exception {

		FileUtils.deleteDirectory(new File("/Local/data/output"));

		JobConf conf = new JobConf(CustomeTextOutputFormat.class);
		conf.setJobName("CustomeTextOutputFormat");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SortMapper.class);
		conf.setOutputKeyComparatorClass(NaturalKeyGroupingComparator.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TopFileOutPutFormat.class);

		FileInputFormat.setInputPaths(conf,
				new Path("/Local/data/employee.csv"));
		FileOutputFormat.setOutputPath(conf, new Path("/Local/data/output"));
		JobClient.runJob(conf);
	}
	
	private static class TopFileOutPutFormat extends MultipleTextOutputFormat<IntWritable,Text>{
		@Override
		protected IntWritable generateActualKey(IntWritable key, Text value) {
			return null;
		}
		
		
	}

}