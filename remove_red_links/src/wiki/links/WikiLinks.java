package wiki.links;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WikiLinks {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
        WikiLinks oWikiLinks = new WikiLinks();

        String input = args[0];
        String output = args[1];
        
        String xmlParseTempLocation = output + "/temp/";;
        String finalLocation = output + "/graph/";

        boolean isCompleted = oWikiLinks.XMLExtraction(input, xmlParseTempLocation);
        if(isCompleted)
        	oWikiLinks.generateGraph(xmlParseTempLocation, finalLocation);
	}
	
    private boolean generateGraph(String secondInput, String finalLocation) throws IOException {
		// TODO Auto-generated method stub
    	try{
            Configuration conf = new Configuration();
            Job graphGenerator = Job.getInstance(conf, "Graph");
            graphGenerator.setJarByClass(WikiLinks.class);

            FileInputFormat.addInputPath(graphGenerator, new Path(secondInput));
            graphGenerator.setInputFormatClass(TextInputFormat.class);
            graphGenerator.setMapperClass(GraphMapper.class);

            FileOutputFormat.setOutputPath(graphGenerator, new Path(finalLocation));
            graphGenerator.setOutputFormatClass(TextOutputFormat.class);

            graphGenerator.setOutputKeyClass(Text.class);
            graphGenerator.setOutputValueClass(Text.class);
            graphGenerator.setReducerClass(GraphReducer.class);

            return graphGenerator.waitForCompletion(true);
        	} catch (Exception e) {
        		throw new IOException(e.getMessage());
    		}
	}

	public boolean XMLExtraction(String input, String output) throws IOException {
		// TODO Auto-generated method stub
    	try{
        Configuration conf = new Configuration();
        
        conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
        conf.set(XMLInputFormat.END_TAG_KEY, "</page>");

        Job xmlParser = Job.getInstance(conf, "XMLParser");
        xmlParser.setJarByClass(WikiLinks.class);

        FileInputFormat.addInputPath(xmlParser, new Path(input));
        xmlParser.setInputFormatClass(XMLInputFormat.class);
        xmlParser.setMapperClass(MapperXMLParser.class);
        
        FileOutputFormat.setOutputPath(xmlParser, new Path(output));
        xmlParser.setOutputFormatClass(TextOutputFormat.class);

        xmlParser.setOutputKeyClass(Text.class);
        xmlParser.setOutputValueClass(Text.class);
        xmlParser.setReducerClass(ReduceXMLParser.class);

        return xmlParser.waitForCompletion(true);
    	} catch (Exception e) {
    		throw new IOException(e.getMessage());
		}
    }
}
