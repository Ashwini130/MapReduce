import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mortbay.jetty.servlet.Context;

public class employeePerLocation {

    public static class EmpByLocMapper extends Mapper<LongWritable,Text,IntWritable,LongWritable>
    {
        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{

            String line = value.toString();
            String[] values = line.split(",");
            if(!values[0].equals("EMPNO"))
            {
                String sal = values[5];
                String dept = values[7];
                System.out.println(sal+" : "+dept);
                context.write(new IntWritable(Integer.parseInt(dept)),new LongWritable(Long.parseLong(sal)));
            }
        }
    }

    public static class EmpByLocReducer extends Reducer<IntWritable,LongWritable,Text,Text>
    {
        HashMap<Integer,String> depLoc = new HashMap<>();
        @Deprecated
        protected void setup(Context context) throws IOException,InterruptedException
        {
            int header = 0;
            try {
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (files != null && files.length > 0) {
                    for (Path dept : files) {
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(dept.toString()));
                        String records = null;
                        while ((records = bufferedReader.readLine()) != null) {
                            String[] cols = records.split(",");
                            if (cols[0].equals("DEPTNO"))
                                continue;
                            else {
                                depLoc.put(Integer.parseInt(cols[0]), cols[2]);
                            }
                        }

                    }
                }
            }catch (Exception e){

            }
        }

        public void reduce(IntWritable key,Iterable<LongWritable> values,Context context) throws IOException,InterruptedException
        {
            long sumSalary = 0;
            int empCount = 0;

            for(LongWritable val : values)
            {
                empCount = empCount+1;
                sumSalary = sumSalary+val.get();
                System.out.println(key.get()+" : "+empCount+" : "+sumSalary);
            }

            Text text = new Text();
            if(depLoc.containsKey(key.get()))
                text.set(depLoc.get(key.get()));
            else text.set("nokeyfound");
            context.write(text,new Text(empCount+","+sumSalary));
        }

    }
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: EmpByLocation <input path> <outputpath> <cache path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(employeePerLocation.class);
        job.setJobName("Emp By Location");


        try {
            DistributedCache.addCacheFile(new Path(args[2]).toUri(),job.getConfiguration());

            //job.addCacheFile(new Path(args[2]).toUri());
        } catch (Exception e) {
            System.out.println(e);
        }
        job.setMapperClass(EmpByLocMapper.class);
        job.setReducerClass(EmpByLocReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
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
