package Mapfunction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.util.HashSet;
import java.util.Set;


@SuppressWarnings("unused")
public class Apriori_one_pase_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	String line;
	String[] tokenizer;
	int type;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		line = value.toString();
		tokenizer =line.split(",");
		
		Set<Set<String>> AllItemSet=new HashSet<Set<String>>();
		Set<Set<String>> onePaseItemSet=new HashSet<Set<String>>();
		
		onePaseItemSet=GetOneCurrItemSet();
		AllItemSet.addAll(onePaseItemSet);
		
		while(onePaseItemSet.size()>1){
			
			onePaseItemSet=CreatNextlevelcurrItem(onePaseItemSet);
			AllItemSet.addAll(onePaseItemSet);
			
		}
		
		for(Set<String> item:AllItemSet){
			
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
	public Set<Set<String>> GetOneCurrItemSet(){
		Set<Set<String>> ItemSet=new HashSet<Set<String>>();
		
		for(int i=0;i<tokenizer.length;i++){
			Set<String>item=new HashSet<String>();
			item.add(tokenizer[i]);
			ItemSet.add(item);
		}
		
		return ItemSet;
	}
	public Set<Set<String>> CreatNextlevelcurrItem(Set<Set<String>> transactionitem){
		Set<Set<String>> loneItemset = new HashSet<Set<String>>(transactionitem);
		Set<Set<String>> NextlevelcurrItem = new HashSet<Set<String>>();
		for(Set<String> set : transactionitem){	
			for(Set<String> set1 : loneItemset){
				if(!set.equals(set1)){
					if(set1.size()==1){
						Set<String> toadd=new HashSet<String>(set);
						toadd.addAll(set1);
						NextlevelcurrItem.add(toadd);
					}
					else
					{
						for(String setitem:set1){
							if(set.contains(setitem))
							{
								Set<String> toadd=new HashSet<String>(set);
								toadd.addAll(set1);
								NextlevelcurrItem.add(toadd);
							}
						}		
					}
				}
			}	
		}
		return NextlevelcurrItem;
	}
}
