package wiki.links;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringEscapeUtils;

public class MapperXMLParser extends Mapper<LongWritable, Text, Text, Text>  {
	
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
        String[] titleAndText = getTextandTitle(value);
        
        String title = titleAndText[0];        
        Text pageTitle = new Text(StringEscapeUtils.unescapeHtml(title.replace(' ', '_').trim()));
        context.write(pageTitle, new Text("~")); 
        // ~ -> random character used to find the red links. Explained in the report.

        Matcher matcher = pattern.matcher(titleAndText[1]);

        while (matcher.find()) {
            String wikiLink = matcher.group();
            wikiLink = wikiLink
            		.replace("[[", "")
                    .replace("]]", "")
                    .replace("|", "")
                    .replace(' ', '_');
            
            context.write(new Text(StringEscapeUtils.unescapeHtml(wikiLink.trim())), pageTitle);
        }
    }
    
    private String[] getTextandTitle(Text value) {
        String[] textandTitle = new String[2];
        String text = value.toString();
        
        int startIndex = text.indexOf("<title>");
        int endIndex = text.indexOf("</title>", startIndex);
        startIndex += 7; 
        
        textandTitle[0] = text.substring(startIndex, endIndex);

        startIndex = text.indexOf("<text"); 
        // Some <text is of the form <text a="x">aaa</text>. We want the part between > and <
        
        startIndex = text.indexOf(">", startIndex); 
        endIndex = text.indexOf("</text>", startIndex);
        startIndex += 1;
        
        if(startIndex == -1 || endIndex == -1) {
            return new String[]{"",""};
        }
        
        textandTitle[1] = text.substring(startIndex, endIndex);
        
        return textandTitle;
    }
}
