package wiki.links;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphReducer extends Reducer<Text, Text, Text, Text>  {
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	String links = "";
    	for(Text val : values){
    		if(!val.toString().trim().equals("~"))
    			links = links + val.toString().trim() + "\t";
    	}
    	context.write(key, new Text(links.toString().trim()));
    }
}
