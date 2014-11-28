package org.apache.hadoopessence.reduceside;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * This sample code demonstrate how normal-join/reduce-side join uses 
 * shuffling and sorting two consolidate data based on field attribute. This is good strategy if both data sets quite
 * large to process on memory. Mapper task will converts both data sets into
 * key-value where key should be join attribute. Reduce will consolidate same
 * key value from both data sets and consolidate it.
 * 
 * 
 * 
 * We have two data sets employee and department which have common fields deptid. Need to fetch the department information for 
 * each employee based on dept id.
 * 
 * 
 * *********Employee***************
 * Empid	EmpName	Deptid
 	1	ABC	11
 	2	XYZ	22
	3	CDE	33
	
***************Department********************
	Deptid	DeptName
	11	Dept A
	22	Dept B
	33	Dept C


**************Employee Department: Join********************
	Empid	EmpName	Deptid	DeptName
	1	ABC	11	Dept A
	2	XYZ	22	Dept B
	3	CDE	33	Dept C
 * 
 */
public class ReduceSideMR {

	/**
	 * 
	 * EmployeeMapper process the employee data sets and converts into key-value
	 * where key is department id and value is empdept key which contains employee and department value both.
	 * We uses index fields to identify if it process employee data sets or department data sets.
	 * if index=0 then employee data sets
	 * if index=0 then department data sets
	 * 
	 */
	public static class EmployeeMapper extends
			Mapper<LongWritable, Text, LongWritable, Empdept> {
		private  LongWritable deptId = new LongWritable();
		private  Empdept keygrpEmp = new Empdept();

		public void map(LongWritable longKey, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] lineArray = line.split(",");
			keygrpEmp = new Empdept();
			keygrpEmp.setIndex(0);
			keygrpEmp.setDeptId(Long.parseLong(lineArray[1]));
			deptId.set(Long.parseLong(lineArray[1]));
			keygrpEmp.setEmpName(lineArray[0]);
			context.write(deptId, keygrpEmp);
		}
	}

	/**
	 * 
	 * DeptMapper process the Department data sets and converts into key-value
	 * where key is department id and value is empdept key which contains employee and department value both.
	 * We uses index fields to identify if it process department data sets or department data sets.
	 * if index=0 then employee data sets
	 * if index=0 then department data sets
	 * 
	 */
	public static class DeptMapper extends
			Mapper<LongWritable, Text, LongWritable, Empdept> {
		private  Empdept keygrpDept = new Empdept();
		private  LongWritable deptId = new LongWritable();
		public void map(LongWritable longKey, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] lineArray = line.split(",");
			keygrpDept = new Empdept();
			keygrpDept.setIndex(1);
			keygrpDept.setDeptId(Long.parseLong(lineArray[0]));
			keygrpDept.setDeptName(lineArray[1]);
			deptId.set(Long.parseLong(lineArray[0]));
			context.write(deptId, keygrpDept);
		}
	}

	/**
	 * 
	 * Reducer is the common reducer which receive out from both mapper EmployeeMapper and DeptMapper.
	 * It sorts and shuffles based on key and pass the same key to the same reducer.
	 * So reducer will be having employee and department for the same key since here we are using key as department number
	 * therefore we can easily merge employee and department details for the same key.
	 * 
	 * 
	 * Here we are making join with Union all. We can also make various join such as inner-join, left outer join,
	 * right outer join etc.
	 * 
	 * 
	 * 
	 * 
	 */
	public static class NormalJoinReducer extends
			Reducer<LongWritable, Empdept, NullWritable, Empdept> {
		public void reduce(LongWritable key, Iterable<Empdept> values,
				Context context) throws IOException, InterruptedException {
			Set<String> listGrp = new HashSet<String>();
			Empdept keyGrp = new Empdept();
			keyGrp.setDeptId(key.get());
			for (Empdept inte : values) {
				if (inte.getIndex() == 1) {
					keyGrp.setDeptName(inte.getDeptName());
				} else {
					listGrp.add(inte.getEmpName());
				}
			}
			for (String empName : listGrp) {
				keyGrp.setEmpName(empName);
				context.write(NullWritable.get(), keyGrp);
			}
		}
	}
	
	/**
	 * 
	 * The job class set the reducer class output key class and output key value.
	 * We are using  MultipleInputs to pass multiple files and with specific mapper.
	 * The will initiate EmployeeMapper and DeptMapper which process and generate key-value pair.
	 * 
	 * 
	 *
	 */

	public class NormalJoinJob extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			FileUtils.deleteDirectory(new File("/Local/data/output"));
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "NormalJoinMR");
			job.setJarByClass(getClass());
			job.setReducerClass(NormalJoinReducer.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Empdept.class);
			

			MultipleInputs.addInputPath(job, new Path(
			 "/Local/data/Employee.csv"), TextInputFormat.class,
			 EmployeeMapper.class);

			 MultipleInputs.addInputPath(job, new Path(
			 "/Local/data/department.csv"), TextInputFormat.class,
			 DeptMapper.class);
			 
			 
			 
			FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			return 0;
		}
	}
	

	public static void main(String[] args) throws Exception {

		Tool tool = new ReduceSideMR().new NormalJoinJob();
		tool.run(null);
	}

	/**
	 * This EmpDept pojo contains employee and Department details.
	 *
	 */
	public static class Empdept implements WritableComparable<Empdept> {

		private Long deptId;
		private Integer index;
		private String deptName;
		private String empName;

		public void write(DataOutput out) throws IOException {
			out.writeLong(deptId);
			out.writeInt(index);
			WritableUtils.writeString(out, deptName);
			WritableUtils.writeString(out, empName);

		}
		public void readFields(DataInput in) throws IOException {
			deptId = in.readLong();
			index = in.readInt();
			deptName=WritableUtils.readString(in);
			empName=WritableUtils.readString(in);

		}
		public int compareTo(Empdept o) {
			int cnt = deptId.compareTo(o.deptId);
			if (cnt == 0) {
				if (index < o.index)
					cnt = -1;
				if (index > o.index)
					cnt = 1;
			}

			return cnt;
		}

		@Override
		public String toString() {
			return this.deptId + "," + this.empName + "," + this.deptName;
		}
		public void setDeptId(long deptId) {
			this.deptId = deptId;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public void setDeptName(String name) {
			this.deptName = name;
		}

		public void setEmpName(String name) {
			this.empName = name;
		}

		public int getIndex() {
			return this.index;
		}

		public String getDeptName() {
			return this.deptName;
		}

		public String getEmpName() {
			return this.empName;
		}

	}
	
}