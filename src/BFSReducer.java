import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by colntrev on 4/4/18.
 */
public class BFSReducer extends Reducer<Text, Text, Text, Text> {
    public static enum Counter {
        FINISHED;
    }
    private Long mapCounterValue;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        mapCounterValue = cluster.getJob(context.getJobID()).getCounters()
                .findCounter(BFSMapper.Counter.FOUND).getValue();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String cons = null;
        Integer dist = Integer.MAX_VALUE;
        String stat = "WHITE";
        if(mapCounterValue == 0){
            // if we haven't found our target node, we need to reduce our map output so that only one key value pair exists
            // for each node and connection
            for(Text value : values){
                String[] tokens = value.toString().split(",");
                Integer distance = Integer.parseInt(tokens[1]);
                if(distance < dist){
                    dist = distance;
                    cons = tokens[0];
                    stat = BFSUtil.checkStatus(stat, tokens[2]);
                }
            }
        } else {
            context.getCounter(Counter.FINISHED).increment(1);
            return;
        }
        String outputString = BFSUtil.output(cons, dist, stat);
        context.write(key, new Text(outputString));
    }


}
