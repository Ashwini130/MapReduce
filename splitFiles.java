import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

public class splitFiles {
    public static class splitMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(!line.split(",")[0].equals("dt")) {
                String year = line.substring(0, 4);
                context.write(new Text(year), new Text(line));
            }
        }
    }

    public static class splitfiles_partitioner extends Partitioner<Text,Text>
    {

        @Override
        public int getPartition(Text key, Text value, int i) {
            int year = Integer.parseInt(key.toString());
            if(year<1799 && year>1700)
                return 0;
            else if(year<1899 && year>1800)
                return 1%i;
            else if(year<1999 && year>1900)
                return 2%i;
            else return 3;
        }
    }


    public static class splitReducer extends Reducer<Text,Text, Text,Text>
    {
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            for(Text val:values)
            {
                String line = val.toString();
                String date = line.split(",")[0];
                String data = line.substring(10);
                context.write(new Text(date),new Text(data));

            }
        }


    }




    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: EmpByLocation <input path> <outputpath> <cache path>");
            System.exit(-1);
        }
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(splitFiles.class);
        job.setJobName("Split files");

        job.setMapperClass(splitMapper.class);
        job.setReducerClass(splitReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setPartitionerClass(splitfiles_partitioner.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
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
