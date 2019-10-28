import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class pm25 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        
	String token_time = tokenizer.nextToken();
        String token_city = tokenizer.nextToken();
	String token_variety = tokenizer.nextToken();
	String token_value_all = "";
        String temp_value = "0";
	while (tokenizer.hasMoreTokens()) {
            String token_value = tokenizer.nextToken();

	   
	    if (token_value.equals(" ")){}
	    else if (!token_value.equals("NR")){
		try{
                    float i = Float.parseFloat(token_value);
                }catch(Exception e){
                    token_value = temp_value;
	        }
	    }

            token_value_all = token_value_all + "," + token_value;
	    temp_value = token_value;
        }

	context.write(new Text(token_time + "," + token_city + "," + token_variety + token_value_all), new Text());
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

	context.write(key, new Text());
    }



 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "Clean");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(WordCount.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
