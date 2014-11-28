package org.apache.hadoopessence.top;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class TopMR {

	public static int intReducerCounter = 0;

	private static class TopMapper extends
			Mapper<LongWritable, Text, NullWritable, Employee> {
		private TreeMap<Employee, Double> employeeMap = new TreeMap<Employee, Double>();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line != null && !line.equalsIgnoreCase("")) {
				String[] empl = line.split(",");
				Employee employee = new Employee();
				Double salary = Double.parseDouble(empl[1]);
				employee.setName(empl[0]);
				employee.setSalary(salary);
				employeeMap.put(employee, salary);
				if (employeeMap.size() > 5) {
					employeeMap.remove(employeeMap.lastKey());
				}
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, NullWritable, Employee>.Context context)
				throws IOException, InterruptedException {
			System.out.println("Mapper Cleanup===" + employeeMap);
			for (Employee emp : employeeMap.keySet()) {
				context.write(NullWritable.get(), emp);
			}

		}

	}

	public static class TopReducer extends
			Reducer<NullWritable, Employee, NullWritable, Employee> {
		private TreeMap<Employee, Double> empMap = new TreeMap<Employee, Double>();

		protected void reduce(NullWritable key, Iterator<Employee> values,
				Context context) throws IOException, InterruptedException {
			int cnt = 0;

			while (values.hasNext()) {
				this.empMap.put(values.next(), new Double(1));
				if (empMap.size() > 5) {
					empMap.remove(empMap.lastKey());
				}
			}

			System.out.println("===" + empMap);
			for (Employee emp : this.empMap.keySet()) {
				context.write(NullWritable.get(), emp);
			}

		}

	}

	public class TopRecordsJob extends Configured implements Tool {

		public int run(String[] arg0) throws Exception {
			FileUtils.deleteDirectory(new File("/Local/data/output"));

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "TopMR");
			job.setJarByClass(TopMR.class);
			job.setNumReduceTasks(1);
			job.setMapperClass(TopMapper.class);
			// /job.setGroupingComparatorClass(EmployeeKeyKeyGroupingComparator.class);
			job.setReducerClass(TopReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Employee.class);
			FileInputFormat.addInputPath(job, new Path(
					"/Local/data/Employee.csv"));
			FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			return 0;
		}

	}

	public static void main(String[] args) throws Exception {

		Tool tool = new TopMR().new TopRecordsJob();
		// FileUtils.deleteDirectory(new File("/Local/data/output"));

		tool.run(null);
	}

	private static class Employee implements WritableComparable<Employee> {

		private String name;
		private Double salary;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Double getSalary() {
			return salary;
		}

		public void setSalary(Double salary) {
			this.salary = salary;
		}

		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, name);
			out.writeDouble(salary);
		}

		public void readFields(DataInput in) throws IOException {

			this.name = WritableUtils.readString(in);
			this.salary = in.readDouble();
		}

		public int compareTo(Employee o) {

			if (o == null)
				return 0;
			int cnt = -1 * this.salary.compareTo(o.salary);
			return cnt == 0 ? this.name.compareTo(o.name) : cnt;
		}

		@Override
		public boolean equals(Object obj) {
			Employee emp = (Employee) obj;
			return (this.name.equalsIgnoreCase(emp.name) && this.salary == emp.salary);

		}

		@Override
		public int hashCode() {
			int hashcode = this.name.hashCode();
			hashcode = hashcode + salary.hashCode();
			return hashcode;
		}

		@Override
		public String toString() {
			return name.toString() + ":" + salary.toString();
		}
	}

}
