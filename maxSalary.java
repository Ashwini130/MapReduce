import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class maxSalary {
    public static class maxSalaryMapper extends Mapper<LongWritable, Text, IntWritable, Text>
    {
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{

            String line = value.toString();
            String[] values = line.split(",");
            if(!values[0].equals("EMPNO"))
            {
                String sal = values[5];
                String empno = values[0];
                String dept = values[7];
                System.out.println(dept+" : "+empno+" : "+sal);
                context.write(new IntWritable(Integer.parseInt(dept)),new Text(empno+":"+sal));
            }
        }
    }

    public static class maxSalaryReducer extends Reducer<IntWritable,Text,IntWritable,Text>
    {
        public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException
        {
            double maxSalary=0;
            String maxempno = null;

            for(Text val : values)
            {
                String[] valcols = val.toString().split(":");
                String empno = valcols[0];
                double sal = Double.parseDouble(valcols[1]);
                if(maxSalary<sal) {
                    maxSalary = sal;
                    maxempno = empno;
                }
            }
            Text text = new Text();
            text.set(maxempno+","+maxSalary);
            context.write(key,text);
        }

    }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: EmpByLocation <input path> <outputpath> <cache path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(employeePerLocation.class);
        job.setJobName("Max Salary with EMPID");


        job.setMapperClass(maxSalaryMapper.class);
        job.setReducerClass(maxSalaryReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
// deleting the output path automatically from hdfs so that we don't have
// to delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);
// exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
