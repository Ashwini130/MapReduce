import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");
            Text newkey = new Text();
            newkey.set(columns[0]);
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            context.write(newkey,new Text(fileName));
        }

    }

    public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            StringBuilder text = new StringBuilder("");
            boolean first = true;
            for(Text val:values)
            {
                if(first)
                    first=false;
                else text.append(" ");

                if(text.lastIndexOf(val.toString())<0)
                    text.append(val.toString());
            }
            context.write(key,new Text(text.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: EmpByLocation <input path> <outputpath> <cache path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(InvertedIndex.class);
        job.setJobName("Inverted Index");

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
