package org.techmytalk.mapreducesample;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * The Class SampleMapReduce.
 */
public class SampleMapReduce {

    public static class SampleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {
        private final static IntWritable cnt = new IntWritable(1);
        private Text text = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            
            text.set(line);
            output.collect(text, NullWritable.get());
            /*StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                text.set(tokenizer.nextToken());
                output.collect(text, cnt);
            }*/
        }
    }

    public static class SampleReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, NullWritable> {

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator,
         * org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, NullWritable> output,
                Reporter reporter) throws IOException {
            int sum = 0;
            //while (values.hasNext()) {
              //  sum += values.next().get();
            //}
            output.collect(key, NullWritable.get());
        }
    }

    /**
     * The main method.
     * 
     * @param args
     *            the arguments
     * @throws Exception
     *             the exception
     */
    public static void main(String[] args) throws Exception {
        
        
        JobConf conf = new JobConf(SampleMapReduce.class);
        conf.setJobName("SampleMapReduce");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(SampleMapper.class);
        conf.setMapOutputValueClass(NullWritable.class);
        ///conf.setCombinerClass(SampleReducer.class);
        conf.setReducerClass(SampleReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("/Local/data/sampleword.csv"));
        FileOutputFormat.setOutputPath(conf, new Path("/Local/data/output"));
        // FileInputFormat.setInputPaths(conf, new Path(inputPath));
        // FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        JobClient.runJob(conf);
    }
}