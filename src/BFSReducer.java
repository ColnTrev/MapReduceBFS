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
                String[] tokens = value.toString().split(";");

                Integer distance = Integer.parseInt(tokens[1]);

                if(!tokens[0].equals("No Connection")){
                    cons = tokens[0];
                }
                if(distance < dist){
                    dist = distance;
                }
                stat = BFSUtil.checkStatus(tokens[2],stat);
            }
            System.out.println("*******REDUCER OUTPUT**********");
            if(stat.equals("GREY")){
                context.getCounter(Counter.FINISHED).increment(1);
            }
        String outputString = BFSUtil.output(cons, dist, stat);
            System.out.println(key + ";" + outputString);
        context.write(null, new Text(key + ";" + outputString));
    }


}
