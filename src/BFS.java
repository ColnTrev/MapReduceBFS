import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by colntrev on 4/4/18.
 */
public class BFS {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job1, job2; // we need three different jobs for this task
        String edgeOutput = "/triangles/edgeResult";
        String triangleOutput = "/triangles/triangleResult";
        String finalOutput = "/triangles/finalOutput";
        Configuration conf = new Configuration();

        job1 = Job.getInstance(conf);
        job1.setJobName("Graph Reader");
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setJarByClass(BFS.class);
        job1.setMapperClass(GraphReadMapper.class);
        job1.setReducerClass(GraphReadReducer.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(edgeOutput));

        job2 = Job.getInstance(conf);
        job2.setJobName("Breadth First Search");
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setJarByClass(BFS.class);
        job2.setMapperClass(BFSMapper.class);
        job2.setReducerClass(BFSReducer.class);
        FileInputFormat.addInputPath(job2, new Path(edgeOutput));
        FileOutputFormat.setOutputPath(job2, new Path(triangleOutput));
        int result = job1.waitForCompletion(true)? 0 : 1;
        if(result == 0){
            result = job2.waitForCompletion(true)? 0 : 1;
        }

        System.exit(result);
    }
}
