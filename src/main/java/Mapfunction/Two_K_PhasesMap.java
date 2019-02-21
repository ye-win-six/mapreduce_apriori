package Mapfunction;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class Two_K_PhasesMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	Configuration conf;
	Set<Set<String>> curItemset;
	Set<String> transactionitem;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
     	
		conf = context.getConfiguration();
     	String curItemStr=conf.get("currentItemstr");
     	String two_phase_part_currentItemstr=conf.get("two_phase_part_currentItemstr");
     	curItemset=new HashSet<Set<String>>();
		curItemset=setcurItemset(curItemStr);
		String[] currentItem=two_phase_part_currentItemstr.split(";");
		for(int i=0;i<currentItem.length;i++){
			word.set(currentItem[i]);
			context.write(word, one);
		}
		
    }
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		String line = value.toString();
		String[] tokenizer =line.split(",");
		transactionitem=new HashSet<String>();
		
		
		for(int i=0;i<tokenizer.length;i++){
			transactionitem.add(tokenizer[i]);
		}
		
		for(Set<String> item:curItemset){
			
			if(transactionitem.containsAll(item)){
				String curitems="";
				for(String curItem:item){
					if(curitems==""){
						curitems=curitems+curItem;
					}else{
						curitems=curitems+","+curItem;
					}
				}
				word.set(curitems);
				context.write(word, one);
			}
		}

	}
	public Set<Set<String>> setcurItemset(String curItemStr){
		Set<Set<String>> curItem=new HashSet<Set<String>>();
		String str[]=curItemStr.split(";");
		for(int i=0;i<str.length;i++){
			String oneitem[]=str[i].split(",");
			Set<String> addset=new HashSet<String>();
			for(int j=0;j<oneitem.length;j++){
				addset.add(oneitem[j]);
			}
			curItem.add(addset);
		}
		return curItem;
	}

}
