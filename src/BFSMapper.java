import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by colntrev on 4/4/18.
 */
public class BFSMapper extends Mapper<Text, Text,Text,Text> {
    public static enum Counter {
        FOUND;
    }
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String node = key.toString();
        String[] valueAsArray = value.toString().split(";");
        String connections = valueAsArray[0];
        Integer distance = Integer.parseInt(valueAsArray[1]);
        String status = valueAsArray[2];
        if(status.equals("GREY")) {
            String[] cons = connections.split(",");
            for (String connection : cons) {
                String nextNode = connection;
                Integer nextDistance = distance + 1;
                String nextStatus = "GREY";
                if (connection.equals(context.getConfiguration().get("targetId"))) {
                    context.getCounter(Counter.FOUND).increment(1);
                }
                String output = BFSUtil.output("No Connection", nextDistance, nextStatus);
                context.write(new Text(nextNode), new Text(output));
            }
            status = "BLACK";
        }
        String outputString = BFSUtil.output(connections, distance, status);
        context.write(new Text(node), new Text(outputString));
    }
}
