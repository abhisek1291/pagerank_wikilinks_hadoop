package wiki.links;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraphMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        
        if(tokens[1].trim().equals("~"))
        	context.write(new Text(tokens[0]), new Text(tokens[1]));
        else
        	context.write(new Text(tokens[1]), new Text(tokens[0]));
    }
}