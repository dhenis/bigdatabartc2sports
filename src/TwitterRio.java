

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

enum CustomCounters {NUM_COMPANIES}

public class TwitterRio { // this will configure the sistem

  public static void runJob(String[] input, String output) throws Exception {

        Configuration conf = new Configuration();

    Job job = new Job(conf);
    //job.setNumReduceTasks(3); // by default will be 2 reduces per job. developer //
    job.setJarByClass(TwitterRio.class);
    job.setMapperClass(TwitterMapper.class);
    //job.setCombinerClass(TwitterReducer.class);
    job.setReducerClass(TwitterReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);


    // gak ada ini
    // job.setOutputFormatClass(SequenceFileOutputFormat.class);
    //
    //
    job.addCacheFile(new Path("/data/medalistsrio.csv").toUri());

    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}
