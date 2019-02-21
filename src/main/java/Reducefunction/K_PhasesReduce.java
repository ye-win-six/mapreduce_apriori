package Reducefunction;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import csie.hadoop.apriori;

@SuppressWarnings("unused")
public class K_PhasesReduce extends Reducer<Text, IntWritable, Text, IntWritable>  {
	
	double support;
	int transactiontotalnumber;
	int minsupport;
	
			
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		support=conf.getDouble("support",0);
		transactiontotalnumber=conf.getInt("totalnumber",0);
		int sum=0;
		
		for (IntWritable num : values) {  
			sum = sum+num.get();  
		}
		
		if(sum >= (int)(support*transactiontotalnumber))
		{
			Set<String> ItemSet=new HashSet<String>();
			ItemSet.add(key.toString());
			context.write(key, new IntWritable(sum));
		}
		
	}
}
