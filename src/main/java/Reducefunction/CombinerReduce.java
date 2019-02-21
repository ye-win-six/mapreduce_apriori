package Reducefunction;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CombinerReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
	{
		int sum= 0;
		for (IntWritable num : values) {  
			sum = sum+num.get();  
		}  
		
		context.write(key, new IntWritable(sum));  
	}
}
