package org.techmytalk.hadoopessence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This Map Reduce job to get count based on group by. In this program we have calculated total population group by
 * country and state on given data on country,state,city,population
 * 
 * input country.csv USA,CA,Sunnyvale,12 USA,CA,SAN JOSE,42 USA,CA,Fremont,23 USA,MO,XY,23 USA,MO,AB,19 USA,MO,XY,23
 * USA,MO,AB,19 IND,TN,AT,11 IND,TN,KL,10
 * 
 * Output IND,TN,84 USA,CA,308 USA,MO,336
 * 
 * 
 */
public class GroupMR {

    /**
     * Map task convert input record into key-value pair and pass to combiner or reducer which are optional. Map-Reduce
     * framework split the file and spawned mapper task for each split.The Number of map task will be decided based on
     * InputSplit defined in InputFormat. InputSplit logically splits input file and each InputSplit is assigned to an
     * individual map task. The split is a logically split not physical splits. The MapReduce first invoke setup()
     * method of context and then invoke map(Object,Object,Context) for each input split and at last invoke
     * cleanup(Context) method.
     */
    public static class GroupMapper extends Mapper<LongWritable, Text, Country, IntWritable> {

        /** The cntry. */
        Country cntry = new Country();

        /** The cnt text. */
        Text cntText = new Text();

        /** The state text. */
        Text stateText = new Text();
        IntWritable populat = new IntWritable();

        /**
         * Map task output grouped on key and sort the individual and finally pass to reducer. The grouping of map
         * output defined by partition which decide which key goes to which reducer. We can create custom Partitioner.
         * MapReduce also provide local combiner which combine intermediate map out and pass to the reducer. It helps to
         * cut down the amount of data transferred from the Map to the Reducer.
         * 
         * Reducer are optional in Map-Reduce if there is no Reducer defined in program then the output of the Mapper
         * directly write to disk without sorting.
         * 
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] keyvalue = line.split(",");
            cntText.set(new Text(keyvalue[0]));
            stateText.set(keyvalue[1]);
            populat.set(Integer.parseInt(keyvalue[3]));
            Country cntry = new Country(cntText, stateText);
            context.write(cntry, populat);

        }
    }

    /**
     * 
     * Reducers copy intermediate map tasks output from local disk through http and pass to individual reducer based on
     * key. Reduce task process collection of value for single key. Reducers shuffle, merge and sort key-value after
     * copying the file from remote node and pass to the reduce tasks.
     * */
    public static class GroupReducer extends Reducer<Country, IntWritable, Country, IntWritable> {

        /**
         * The map task out will pass to reduce which will spawned reduce() method for each key.
         * 
         * 
         */
        public void reduce(Country key, Iterator<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            int cnt = 0;
            while (values.hasNext()) {
                cnt = cnt + values.next().get();
            }
            context.write(key, new IntWritable(cnt));

        }

    }

    /**
     * 
     * The Country class implements WritabelComparator to implements custom sorting to perform group by operation. It
     * sorts country and then state.
     * 
     */
    private static class Country implements WritableComparable<Country> {

        Text country;
        Text state;

        public Country(Text country, Text state) {
            this.country = country;
            this.state = state;
        }
        public Country() {
            this.country = new Text();
            this.state = new Text();

        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
         */
        public void write(DataOutput out) throws IOException {
            this.country.write(out);
            this.state.write(out);

        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
         */
        public void readFields(DataInput in) throws IOException {

            this.country.readFields(in);
            this.state.readFields(in);
            ;

        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        public int compareTo(Country pop) {
            if (pop == null)
                return 0;
            int intcnt = country.compareTo(pop.country);
            if (intcnt != 0) {
                return intcnt;
            } else {
                return state.compareTo(pop.state);

            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {

            return country.toString() + ":" + state.toString();
        }

    }

    /**
     * This main method call the MapReduce Job. Before calling the job we need to set the MapperClass, ReducerClas,
     * OutputKeyClass and OutputValueClass. We can also set the FileInputFormat and FileOutputFormat.
     * 
     * FileInputFormat use to decide number of map tasks we can also set input split size which also take part while
     * deciding number of map task.
     * 
     * Split size can min between maxSize and blockSizeMath.min(maxSize, blockSize)
     * 
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        FileUtils.deleteDirectory(new File("/Local/data/output"));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "GroupMR");
        job.setJarByClass(GroupMR.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Country.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setMaxInputSplitSize(job, 10);
        FileInputFormat.setMinInputSplitSize(job, 100);
        FileInputFormat.addInputPath(job, new Path("/Local/data/Country.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
