import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class lineCounter {

    public static enum person{
        DUNCAN,MALCOLM
    }
    public static class lcMapper extends Mapper<LongWritable, Text,Text, IntWritable>
    {
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            Text text = new Text();
            if(line.startsWith("DUNCAN")) {
                text.set("DUNCAN");
                context.getCounter(person.DUNCAN).increment(1);
            }
            if(line.startsWith("MALCOLM")) {
                text.set("MALCOLM");
                context.getCounter(person.MALCOLM).increment(1);
            }
            System.out.println(text.toString()+" "+1);
            context.write(text,new IntWritable(1));
        }
    }

    public static class lcReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum =0;
            for(IntWritable i:values)
            {
                sum = sum + i.get();
            }
            context.write(key,new IntWritable(sum));

        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: EmpByLocation <input path> <outputpath> <cache path>");
            System.exit(-1);
        }
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(lineCounter.class);
        job.setJobName("line counter");

        job.setMapperClass(lcMapper.class);
        job.setReducerClass(lcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
// deleting the output path automatically from hdfs so that we don't have
// to delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);
// exiting the job only if the flag value becomes false
        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            Counters counters = job.getCounters();

            Counter c1 =  counters.findCounter(person.DUNCAN);
            System.out.println(c1.getDisplayName()+" = "+c1.getValue());
            c1 = counters.findCounter(person.MALCOLM);
            System.out.println(c1.getDisplayName()+" = "+c1.getValue());
        }
        System.exit(0);
    }
    }

