package Mapfunction;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import csie.hadoop.apriori;

@SuppressWarnings("unused")
public class OnePhasesMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	String line;
	String[] tokenizer;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		line = value.toString();
		tokenizer =line.split(",");
		
		for(int i=0;i<tokenizer.length;i++){
			word.set(tokenizer[i]);
			context.write(word, one);
		}
		
	}
}
