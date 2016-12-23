package wiki.links;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceXMLParser extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	HashSet<String> hs = new HashSet<String>();
    	int count = 0;
        for (Text value : values) {
        	hs.add(value.toString());  
        	count++;
        }
        
        if(hs.contains("~")){
        	for (String s : hs) {
        	    if(!key.toString().trim().equals(s.trim())){
                	context.write(key, new Text(s));
        	    }
        	}
        }
    }
}
