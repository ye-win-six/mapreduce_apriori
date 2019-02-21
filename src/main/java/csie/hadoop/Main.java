package csie.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {
	//double support = 0.3 ,confidence = 0.05 ;
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		double support = Double.parseDouble(args[2]);
		
		FAMR_apriori FAMR_apri=new FAMR_apriori(args[0],args[1],support);
		FAMR_apri.one_phase_FAMR_Apriori();
		FAMR_apri.K_phase_FAMR_Apriori();
		
		apriori apri=new apriori();
		apri.one_phase_doApriori(args[0], args[1], support);
		apri.K_phase_doApriori(args[0], args[1], support);
	}
	

}
