import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by colntrev on 4/4/18.
 */
public class BFSMapper extends Mapper<LongWritable, Text,Text,Text> {
    public static enum Counter {
        FOUND;
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] valueAsArray = value.toString().split(";");

        String node = valueAsArray[0];
        String connections = valueAsArray[1];
        Integer distance = Integer.parseInt(valueAsArray[2]);
        String status = valueAsArray[3];
        if(status.equals("GREY")) {
            String[] cons = connections.split(",");
            for (String connection : cons) {
                String nextNode = connection;
                Integer nextDistance = distance + 1;
                String nextStatus = "GREY";
                String output = BFSUtil.output("No Connection", nextDistance, nextStatus);
                context.write(new Text(nextNode), new Text(output));
            }
            status = "BLACK";
        }
        String outputString = BFSUtil.output(connections, distance, status);
        context.write(new Text(node), new Text(outputString));
    }
}
