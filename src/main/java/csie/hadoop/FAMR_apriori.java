package csie.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Mapfunction.Apriori_one_pase_Map;
import Mapfunction.K_PhasesMap;
import Mapfunction.OnePhasesMap;
import Mapfunction.Two_K_PhasesMap;
import Reducefunction.CombinerReduce;
import Reducefunction.K_PhasesReduce;

@SuppressWarnings("unused")
public class FAMR_apriori {
	Map<String,ArrayList<Integer>> AprioriTID_Map;
	Map<Integer,ArrayList<String>> fileMap;
	String KphaseoutputDir,outputDir,inputDir,currentItemstr="",two_phase_part_currentItemstr="";
	Set<Set<String>> currentItemset,AllLargeItemSet,largeItemSet;
	ArrayList<String> two_phase_part_currentItemset;
	Configuration conf;
	double minsupport;
	int readercount=0,largenumber=1;
	
	public FAMR_apriori(String inputDir,String outputDir,double minsupport)
	{
		AprioriTID_Map = new HashMap<String,ArrayList<Integer>>();
		fileMap=new HashMap<Integer,ArrayList<String>>();
		two_phase_part_currentItemset=new ArrayList<String>();
		conf = new Configuration();
		this.inputDir=inputDir;
		this.outputDir=outputDir;
		this.minsupport=minsupport;
	}
	
	public void one_phase_FAMR_Apriori() throws Exception{
		
		parsefile("/home/yewinlix/input");
		one_phase_MapReduce();
		
	}
	
	public void K_phase_FAMR_Apriori() throws Exception{
		
		currentItemset=new HashSet<Set<String>>();
		largeItemSet=new HashSet<Set<String>>();
		AllLargeItemSet=new HashSet<Set<String>>();
		
		
		parsefile("/home/yewinlix/input");
		KphaseoutputDir=outputDir+largenumber;
		GetOneLargeItemSet();
		
		conf.setDouble("support", minsupport);
		conf.setInt("totalnumber", readercount);
		Get_two_LargeItemSet();
		
		while( currentItemset.size() > 0)
		{
			creatNextleveLargeItemSet();
			largenumber++;
			KphaseoutputDir=outputDir+largenumber;
		}
		
		System.out.println("...................");
		System.out.println(AllLargeItemSet);
	}
	
	public void one_phase_MapReduce() throws Exception{
		
		conf.setDouble("support", minsupport);
		conf.setInt("totalnumber", readercount);
		Job job = new Job(conf);
		job.setNumReduceTasks(2);
		job.setJarByClass(FAMR_apriori.class);
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
	
	public void GetOneLargeItemSet() throws Exception{
		
		Set<String> item;
		for(String key:AprioriTID_Map.keySet()){
			if(AprioriTID_Map.get(key).size() >= (int)(minsupport*readercount))
			{
				item=new HashSet<String>();
				item.add(key);
				AllLargeItemSet.add(item);
			}
		}
		largeItemSet=AllLargeItemSet;
		currentItemset=CreatNextlevelcurrItem(largeItemSet);
		currentItemstr=setcurrentItemstr(currentItemset);
		
		largenumber++;
		KphaseoutputDir=outputDir+largenumber;
	}
	
	
	public void Get_two_LargeItemSet()throws Exception{
		conf.set("two_phase_part_currentItemstr", two_phase_part_currentItemstr);
		conf.set("currentItemstr", currentItemstr);
		Job job = new Job(conf);	
		job.setNumReduceTasks(2);
		job.setJarByClass(FAMR_apriori.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Mapfunction.Two_K_PhasesMap.class);
		job.setReducerClass(Reducefunction.K_PhasesReduce.class);
		job.setCombinerClass(Reducefunction.CombinerReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("gs://musthadoopdata/input/"+inputDir));
		FileOutputFormat.setOutputPath(job, new Path("gs://musthadoopdata/output/"+KphaseoutputDir));

		job.waitForCompletion(true);
		
		largeItemSet=readFrequentItemsFromFile();
		currentItemset=CreatNextlevelcurrItem(largeItemSet);
		currentItemstr=setcurrentItemstr(currentItemset);
		largenumber++;
		KphaseoutputDir=outputDir+largenumber;
	}
	
	public void creatNextleveLargeItemSet()throws Exception 
	{
	
		conf.set("currentItemstr", currentItemstr);
		Job job = new Job(conf);	
		job.setNumReduceTasks(2);
		job.setJarByClass(FAMR_apriori.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Mapfunction.K_PhasesMap.class);
		job.setReducerClass(Reducefunction.K_PhasesReduce.class);
		job.setCombinerClass(Reducefunction.CombinerReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("gs://musthadoopdata/input/"+inputDir));
		FileOutputFormat.setOutputPath(job, new Path("gs://musthadoopdata/output/"+KphaseoutputDir));

		job.waitForCompletion(true);
		
		largeItemSet=readFrequentItemsFromFile();
		currentItemset=CreatNextlevelcurrItem(largeItemSet);
		currentItemstr=setcurrentItemstr(currentItemset);
	}
	
	public  void parsefile(String filename) throws IOException{
		
		File fDir=new File(filename);
		File[] filenamelist=fDir.listFiles();
		
		for(int i=0;i<fDir.listFiles().length;i++){
			BufferedReader red = new BufferedReader(new FileReader(filenamelist[i]));
			String readlineinput="";
			Set<String> lineSet;
			
			while((readlineinput = red.readLine())!= null)
			{
					++readercount;
					lineSet = new HashSet<String>();
					String temp[] = readlineinput.split(",");
					
					for( int j=0;j<temp.length;j++)
					{
						 UpdateAprioriTID_Map(temp[j],readercount);
					}
			}
		}
		creatFAMRfileMap();
		fileMap=FAMR_Data_Preprocess(fileMap);//K_phase_FAMR_Use
		System.out.println("SUCS");
		
	}
	
	public  void UpdateAprioriTID_Map(String item,int readercount)
	{
			ArrayList<Integer> number;
			if (AprioriTID_Map.containsKey(item) == false)
			{
				number=new ArrayList<Integer>();
				number.add(readercount);
				AprioriTID_Map.put(item,number);
			}
			else 
			{
				number=AprioriTID_Map.get(item);
				number.add(readercount);
				AprioriTID_Map.put(item, number);
			}
			
	}
	public void creatFAMRfileMap(){
		for(String key:AprioriTID_Map.keySet())
		{
			if(AprioriTID_Map.get(key).size()>=(int)(minsupport*readercount))
			{
				Updatefilemap(key,AprioriTID_Map.get(key));
			}
		}
	}
	
	public void Updatefilemap(String item,ArrayList<Integer> num){
		ArrayList<String> ItemList;
		for(int i=0;i<num.size();i++)
		{
			if(fileMap.containsKey(num.get(i))==false)
			{
				ItemList=new ArrayList<String>();
				ItemList.add(item);
				fileMap.put(num.get(i),ItemList);
			}
			else 
			{
				ItemList=fileMap.get(num.get(i));
				ItemList.add(item);
				fileMap.put(num.get(i),ItemList);
			}
		}
	}
	
	public Map<Integer,ArrayList<String>> FAMR_Data_Preprocess(Map<Integer,ArrayList<String>> fileMap){
		Map<Integer,ArrayList<String>> Data_PreprocessMap=new HashMap<Integer,ArrayList<String>>(fileMap);
		for(Integer key:fileMap.keySet()){
			if(fileMap.get(key).size()==1)
			{
				Data_PreprocessMap.remove(key);
			}
			else if(fileMap.get(key).size()==2)
			{
				String item="";
				for(int i=0;i<fileMap.get(key).size();i++){
					if(i == (fileMap.get(key).size()-1)){
						item=item+fileMap.get(key).get(i);
					}
					else
					{
						item=item+fileMap.get(key).get(i)+",";
					}
				}
				two_phase_part_currentItemset.add(item);
				Data_PreprocessMap.remove(key);
			}
		}
		two_phase_part_currentItemstr=set_two_phase_part_currentItemstr(two_phase_part_currentItemset);
		return Data_PreprocessMap;
	}
	
	public  void writefile()
	{
		
		try{
			BufferedWriter wr=new BufferedWriter(new FileWriter("/home/yewinlix/FAMR_input/data.txt"));
			
			for (Integer key: fileMap.keySet()) {
				for(int i=0;i<fileMap.get(key).size();i++)
				{
					wr.write(fileMap.get(key).get(i));
					if(i != (fileMap.get(key).size()-1)){
						wr.write(",");
					}
				}
				wr.write("\r\n");
			} 
			
			wr.flush();  
			wr.close();
			System.out.println("SUCS");
			UpdateGoogleCloudStorage();
		}
		catch(Exception ex)
		{
			System.out.println("fail");
		}
		
	}
	public void UpdateGoogleCloudStorage() throws InterruptedException, IOException
	{
	        new ProcessBuilder("gsutil", "cp", "file:///home/yewinlix/FAMR_input/data.txt", "gs://musthadoopdata/input/"+inputDir).inheritIO().start().waitFor();
	        System.out.println("SUCC");
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
	
	public String set_two_phase_part_currentItemstr(ArrayList<String>Itemset){
		String item="";
		for(String set : Itemset){	
			item=item+set+";";
		}
		return item;
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
	
	public Set<Set<String>> readFrequentItemsFromFile()throws Exception{
		
		rundownloadoutputfile();
		Set<Set<String>> itemset=new HashSet<Set<String>>();
		File fDir=new File("/home/yewinlix/output/"+KphaseoutputDir);
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
				AllLargeItemSet.add(item);
			}
		}
		return itemset;
	}
	public void rundownloadoutputfile() throws Exception{
       
		new ProcessBuilder("gsutil", "-m", "cp", "-r",
        	"gs://musthadoopdata/output/"+KphaseoutputDir,
            "file:///home/yewinlix/output/")
        .inheritIO()
        .start()
        .waitFor();
        System.out.println("SUCC");
	}

}
