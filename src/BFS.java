import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
        int iteration = 1;

        Job job1, job2; // we need three different jobs for this task
        String edgeOutput = "hdfs:///user/bfs/graphReader";
        String triangleOutput = "hdfs:///user/bfs/bfsResult_1";
        Configuration conf = new Configuration();
        conf.set("sourceId", "1");

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
        FileInputFormat.addInputPath(job1, new Path("hdfs:///user/bfsdata.txt"));
        FileOutputFormat.setOutputPath(job1, new Path(edgeOutput));

        job2 = Job.getInstance(conf);
        job2.setJobName("Breadth First Search" + iteration);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setJarByClass(BFS.class);
        job2.setMapperClass(BFSMapper.class);
        job2.setReducerClass(BFSReducer.class);

        Path in;
        Path out = new Path("hdfs:///user/bfs/bfsResult_1/");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(out)){
            fs.delete(out,true);
        }
        FileInputFormat.addInputPath(job2, new Path(edgeOutput));
        FileOutputFormat.setOutputPath(job2, out);

        long startTime = System.currentTimeMillis();
        int result = job1.waitForCompletion(true)? 0 : 1;
        if(result == 0){
            job2.waitForCompletion(true);
            iteration++;
            long finished = job2.getCounters().findCounter(BFSReducer.Counter.FINISHED).getValue();
            while(finished > 0){
                conf = new Configuration();

                conf.set("sourceId", "1");
                job2 = Job.getInstance(conf);
                job2.setJobName("BFS " + iteration);
                job2.setJarByClass(BFS.class);
                job2.setMapperClass(BFSMapper.class);
                job2.setReducerClass(BFSReducer.class);

                in = new Path("hdfs:///user/bfs/bfsResult_" + (iteration - 1) + "/");
                out = new Path("hdfs:///user/bfs/bfsResult_" + iteration + "/");

                FileInputFormat.addInputPath(job2, in);

                if(fs.exists(out)){
                    fs.delete(out,true);
                }

                FileOutputFormat.setOutputPath(job2,out);
                job2.setInputFormatClass(TextInputFormat.class);
                job2.setOutputFormatClass(TextOutputFormat.class);

                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);

                job2.waitForCompletion(true);
                iteration++;
                finished = job2.getCounters().findCounter(BFSReducer.Counter.FINISHED).getValue();
                System.out.println("COUNTER STATUS: " + finished);
            }
            System.out.println(finished);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Elapsed Time: " + (endTime - startTime));

        System.exit(result);
    }
}
