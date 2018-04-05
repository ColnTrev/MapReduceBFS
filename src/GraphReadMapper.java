import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by colntrev on 4/4/18.
 */
public class GraphReadMapper extends Mapper<Text,Text,Text,Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(";");
        String node = tokens[0];
        Integer distance = Integer.MAX_VALUE;
        String status = "WHITE";
        if(node.equals(context.getConfiguration().get("sourceId"))){
            distance = 0;
            status = "GREY";
        }
        String outputString = BFSUtil.output(tokens[1], distance, status);
        context.write(new Text(node), new Text(outputString));
    }
}
