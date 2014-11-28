package org.apache.hadoopessence.mapside;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * This sample code demonstrate how mapside join two data sets based on field attribute.
 * This strategy useful if one data sets small to fit in memory. In this example we uses 
 * distributed cache to load small file in memory. Small file first get process and load 
 * into memory then this will agregrate with large data sets. 
 * 
 * 
 * 
 * We have two data sets employee and department which have common fields
 * deptid whereas department file is small to fit in memory.
 * 
 * 
 * *********Employee*************** 
 * EmpName Deptid 
 * ABC 11 
 * XYZ 22 
 * CDE 33
 * 
 *************** Department******************** 
 *Deptid DeptName 
 *11 DeptA 
 *22 DeptB 
 *33 DeptC
 * 
 * 
 ************** Employee Department: Join******************** 
 * Deptid, EmpName, DeptName 
 * 11,ABC, DeptA 
 * 22,XYZ, DeptB
 * 33,CDE, DeptC
 * 
 */
public class MapSideJoinMR {
	/**
	 * EmployeeMapper process the employee data sets and converts into key-value
	 * where key is department id and value is employee name.
	 */
	public static class MapSideMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Map<String, String> deptMap = new HashMap<String, String>();
		private Text txtValue = new Text();
		private Text txtKey = new Text();

		/**
		 * Setup method called once before mapper task get invoked. Setup will fetch 
		 * department file from distributed cache and convert into map<key,value> where 
		 * key id department id and value is department name.
		 * 
		 */
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			FileSystem dfs = FileSystem.get(context.getConfiguration());
			Path[] cacheFilePath = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (int i = 0; i < cacheFilePath.length; i++) {
				cacheFilePath[i].toString();
				BufferedReader bufreader = new BufferedReader(
						new InputStreamReader(dfs.open(cacheFilePath[i])));
				String line = bufreader.readLine();
				String[] splitline = {};
				while (line != null) {
					try {
						line = line.replaceAll("\\n", "");
						splitline = line.split(",");
						deptMap.put(splitline[0], splitline[1]);
						line = bufreader.readLine();
					} catch (Exception e) {
						System.out.println("Error in line=" + line);
					}
				}

			}
		}
		/**
		 *It will check with populated department map and if it gets department id then 
		 *aggregrate the employee with department.
		 */
		public void map(LongWritable longKey, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] lineArray = line.split(",");
			String deptName = deptMap.get(lineArray[1]);
			if (deptName != null && !deptName.equals("")) {
				txtKey.set(lineArray[1]);
				txtValue.set(lineArray[0] + "\t" + deptName);
				context.write(txtKey, txtValue);
			}
		}
	}

	/**
	 * 
	 * The job class set the reducer class output key class and output key
	 * value. We are using MultipleInputs to pass multiple files and with
	 * specific mapper. The will initiate EmployeeMapper and DeptMapper which
	 * process and generate key-value pair.
	 */

	public class MapSideJoinJob extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			FileUtils.deleteDirectory(new File("/Local/data/output"));
			Job job = Job.getInstance(new Configuration(), "MapSideJoin");
			job.setJarByClass(getClass());
			job.setMapperClass(MapSideMapper.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			Configuration conf = job.getConfiguration();
			DistributedCache.addCacheFile(new URI(
					"/Local/data/department.csv#department.csv"), conf);
			FileInputFormat.setInputPaths(job, new Path(
					"/Local/data/Employee.csv"));

			FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			return 0;
		}
	}

	public static void main(String[] args) throws Exception {
		Tool tool = new MapSideJoinMR().new MapSideJoinJob();
		tool.run(null);
	}
}