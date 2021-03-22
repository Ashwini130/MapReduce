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
import java.util.Locale;

public class wordsFrequency {
    public static class wfMapper extends Mapper<LongWritable, Text,Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] words = str.split("[\\[/)\"(+!, ?.:;'-]+");
            for(String word : words)
            {
                word = word.toLowerCase(Locale.ROOT);
                context.write(new Text(word),new IntWritable(1));
            }
        }

    }

    public static class wfReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            if(key.toString().length()>=5) {
                int sum = 0;
                for (IntWritable val : values) {
                    sum = sum + val.get();
                }
                context.write(key,new IntWritable(sum));
            }

        }

    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: EmpByLocation <input path> <outputpath> <cache path>");
            System.exit(-1);
        }
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(wordsFrequency.class);
        job.setJobName("Words Frequency");

        job.setMapperClass(wfMapper.class);
        job.setReducerClass(wfReducer.class);
        job.setCombinerClass(wfReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
