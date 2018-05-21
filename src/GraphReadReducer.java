import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by colntrev on 4/4/18.
 */
public class GraphReadReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value : values){
            StringBuilder sb = new StringBuilder();
            sb.append(key.toString());
            sb.append(';');
            sb.append(value);
            context.write(null,new Text(sb.toString()));
        }
    }
}
