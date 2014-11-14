package org.techmytalk.mapreducesample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopEmployee {

    private static class EmployeeMapper extends Mapper<LongWritable, Text, DoubleWritable, Employee> {
        TreeMap<Employee, DoubleWritable> fatcats = new TreeMap<Employee, DoubleWritable>();

        @Override
        protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (line != null && !line.equalsIgnoreCase("")) {
                String[] empl = line.split(",");
                Employee employee = new Employee();
                DoubleWritable dblwrtr = new DoubleWritable(Double.parseDouble(empl[1]));
                employee.setName(new Text(empl[0]));
                employee.setSalary(dblwrtr);

                fatcats.put(employee, dblwrtr);

                System.out.println("==" + empl[0] + "===" + empl[1] + "==" + employee.toString());

            }
        }

        @Override
        protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
                InterruptedException {
            int cnt = 0;
            for (Employee value : fatcats.keySet()) {
                cnt++;
                if (cnt > 5)
                    return;
                context.write(new DoubleWritable(0), value);
            }

        }
    }

    public static class Employee implements WritableComparable<Employee> {

        private Text name = new Text();

        public Text getName() {
            return name;
        }

        public void setName(Text name) {
            this.name = name;
        }

        public DoubleWritable getSalary() {
            return salary;
        }

        public void setSalary(DoubleWritable salary) {
            this.salary = salary;
        }

        public DoubleWritable salary = new DoubleWritable();

        public void write(DataOutput out) throws IOException {
            this.name.write(out);
            this.salary.write(out);

        }

        public void readFields(DataInput in) throws IOException {
            this.name.readFields(in);
            this.salary.readFields(in);

        }

        public int compareTo(Employee o) {
            int retcnt = 0;
            if (o == null)
                return 0;
            retcnt = o.getSalary().compareTo(salary);
            if (retcnt == 0)
                retcnt = o.name.compareTo(name);
            return -1 * retcnt;
        }

        @Override
        public String toString() {
            return name.toString() + ":" + salary.toString();
        }
    }

    public static class EmployeeReducer extends Reducer<DoubleWritable, Employee, DoubleWritable, Employee> {

        protected void reduce(DoubleWritable key, Iterator<Employee> values,
                org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
            while (values.hasNext()) {
                context.write(key, values.next());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileUtils.deleteDirectory(new File("/Local/data/output"));
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(TopEmployee.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(EmployeeMapper.class);
        job.setReducerClass(EmployeeReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Employee.class);
        FileInputFormat.addInputPath(job, new Path("/Local/data/Employee.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Local/data/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
