package csie.hadoop;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.io.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Mapfunction.Apriori_one_pase_Map;
import Mapfunction.K_PhasesMap;
import Mapfunction.OnePhasesMap;
import Reducefunction.CombinerReduce;
import Reducefunction.K_PhasesReduce;

public class apriori {
	
	int largenumber=1,transactiontotalnumber;
	double support,confidence;
	
	String outputDir,inputDir;
	String currentItemstr="";
	Set<Set<String>> currentItemset;
	Set<Set<String>> largeItemSet;
	Map<Set<String>,Integer> ALLlargeItemsetsupport;
	Configuration conf;
	
	public apriori(){
		
		currentItemset=new HashSet<Set<String>>();
		largeItemSet=new HashSet<Set<String>>();
		ALLlargeItemsetsupport=new HashMap<Set<String>,Integer>();
		conf = new Configuration();
	}
	
	public void K_phase_doApriori(String input,String output,double sup) throws Exception 
	{
		
		support=sup;	
		inputDir=input;
		outputDir=output+largenumber;
		transactiontotalnumber=gettransactiontotalnumber();
		conf.setDouble("support", support);
		conf.setInt("totalnumber", transactiontotalnumber);
		
		
		GetOneLargeItemSet();
		largenumber++;
		outputDir=output+largenumber;
		
		while( currentItemset.size() > 0)
		{
			creatNextleveLargeItemSet();
			largenumber++;
			outputDir=output+largenumber;
		}
		
		for(Set<String> key:ALLlargeItemsetsupport.keySet()){
			System.out.println(key+":"+ALLlargeItemsetsupport.get(key));
		}
		
	}
	
	public void one_phase_doApriori(String input,String output,double sup) throws Exception 
	{
		support = sup;	
		inputDir=input;
		outputDir=output;
		transactiontotalnumber=gettransactiontotalnumber();
		conf.setDouble("support", support);
		conf.setInt("totalnumber", transactiontotalnumber);
		Another_Apriori_Algorithm_on_MapReduce();
		
	}
	
	public void GetOneLargeItemSet() throws Exception{
		
		Job job = new Job(conf);	
		job.setNumReduceTasks(2);
		job.setJarByClass(apriori.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Mapfunction.OnePhasesMap.class);
		job.setReducerClass(Reducefunction.K_PhasesReduce.class);
		job.setCombinerClass(Reducefunction.CombinerReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("gs://musthadoopdata/input/"+inputDir));
		FileOutputFormat.setOutputPath(job, new Path("gs://musthadoopdata/output/"+outputDir));

		job.waitForCompletion(true);
		
		largeItemSet=readFrequentItemsFromFile();
		currentItemset=CreatNextlevelcurrItem(largeItemSet);
		currentItemstr=setcurrentItemstr(currentItemset);
		
	}
	public void creatNextleveLargeItemSet()throws Exception 
	{
		
		conf.set("currentItemstr", currentItemstr);
		Job job = new Job(conf);	
		job.setNumReduceTasks(2);
		job.setJarByClass(apriori.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Mapfunction.K_PhasesMap.class);
		job.setReducerClass(Reducefunction.K_PhasesReduce.class);
		job.setCombinerClass(Reducefunction.CombinerReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("gs://musthadoopdata/input/"+inputDir));
		FileOutputFormat.setOutputPath(job, new Path("gs://musthadoopdata/output/"+outputDir));

		job.waitForCompletion(true);
		
		largeItemSet=readFrequentItemsFromFile();
		currentItemset=CreatNextlevelcurrItem(largeItemSet);
		currentItemstr=setcurrentItemstr(currentItemset);
	}
	
	public Set<Set<String>> CreatNextlevelcurrItem(Set<Set<String>> largeItem){
		Set<Set<String>> loneItemset = new HashSet<Set<String>>(largeItem);
		Set<Set<String>> NextlevelcurrItem = new HashSet<Set<String>>();
		for(Set<String> set : largeItem){	
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
	public String setcurrentItemstr(Set<Set<String>>Itemset){
		String item="";
		for(Set<String> set : Itemset){	
			for(String str:set){
				item=item+str+",";
			}
			item=item+";";
		}
		return item;
	}
	public Set<Set<String>> readFrequentItemsFromFile()throws Exception{
		rundownloadoutputfile();
		Set<Set<String>> itemset=new HashSet<Set<String>>();;
		File fDir=new File("/home/fhbgn037561021/output/"+outputDir);
		File[] filename=fDir.listFiles();
		
		for(int i=0;i<fDir.listFiles().length;i++){
			BufferedReader red = new BufferedReader(new FileReader(filename[i]));
			String readlineinput="";
			
			while((readlineinput = red.readLine())!= null){
				Set<String> item=new  HashSet<String>();
				String temp[] = readlineinput.split("\\t");
				int itemcoun=Integer.parseInt(temp[1]);
				String tempitem[] = temp[0].split(",");
				
				for(int j=0;j<tempitem.length;j++){
					item.add(tempitem[j]);
				}
	
				itemset.add(item);
				ALLlargeItemsetsupport.put(item,itemcoun);
			}
		}
		return itemset;
	}
	public void rundownloadoutputfile() throws Exception{
	        new ProcessBuilder("gsutil", "-m", "cp", "-r",
	        	"gs://musthadoopdata/output/"+outputDir,
	            "file:///home/fhbgn037561021/output/")
	        .inheritIO()
	        .start()
	        .waitFor();
	        System.out.println("SUCC");
	}
	public int gettransactiontotalnumber()throws Exception{
		int totalnumber=0;
		File fDir=new File("/home/fhbgn037561021/input/");
		File[] filename=fDir.listFiles();
		
		for(int i=0;i<fDir.listFiles().length;i++){
			BufferedReader red = new BufferedReader(new FileReader(filename[i]));
			String readlineinput="";
			while((readlineinput = red.readLine())!= null){
				totalnumber++;
			}
		}
		return totalnumber;
	}
	
	public void Another_Apriori_Algorithm_on_MapReduce() throws Exception{
		//public void GetOneLargeItemSet(String Input,String Output, Configuration conf) throws Exception{
		Job job = new Job(conf);
		job.setNumReduceTasks(2);
		job.setJarByClass(apriori.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Mapfunction.Apriori_one_pase_Map.class);
		job.setReducerClass(Reducefunction.K_PhasesReduce.class);
		job.setCombinerClass(Reducefunction.CombinerReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("gs://musthadoopdata/input/"+inputDir));
		FileOutputFormat.setOutputPath(job, new Path("gs://musthadoopdata/output/"+outputDir));

		job.waitForCompletion(true);
	}
	
}
