import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by colntrev on 4/4/18.
 */
public class BFSReducer extends Reducer<Text, Text, Text, Text> {
    public static enum Counter {
        FINISHED;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String cons = null;
        Integer dist = Integer.MAX_VALUE;
        String stat = "WHITE";
            for(Text value : values){
                if(value.toString().equals("FOUND") || context.getCounter(Counter.FINISHED).getValue() > 0){
                    context.getCounter(Counter.FINISHED).increment(1);
                    return;
                }
                String[] tokens = value.toString().split(";");
                Integer distance = Integer.parseInt(tokens[1]);
                if(distance < dist){
                    dist = distance;
                    cons = tokens[0];
                    stat = BFSUtil.checkStatus(stat, tokens[2]);
                }
            }
        String outputString = BFSUtil.output(cons, dist, stat);
        context.write(key, new Text(outputString));
    }


}
