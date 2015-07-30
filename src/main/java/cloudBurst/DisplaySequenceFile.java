package cloudBurst;//Written by Alexander Mont <alexmont1@comcast.net>

//Reads a SequenceFile and outputs the key-value pairs. Intended
//primarily for testing and debugging purposes.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileWriter;
//TODO migrate to yarn - DONE

public class DisplaySequenceFile {
   public static void main(String[] args) throws Exception
   {
	   String filename = null;
	   
	   int data = 1;
	   
	   if (data == 1)
	   {
		   filename = "/user/guest/cloudburst/s_suis.br";
	   }
	   else
	   {
		   if (args.length != 1) {
			   System.err.println("Usage: DisplaySequenceFile seqfile");
			   System.exit(-1);
		   }

		   filename = args[0];
	   }
		
		System.err.println("Printing " + filename);
		
		
       Path thePath = new Path(filename);
       //JobConf conf = new JobConf(DisplaySequenceFile.class);
       Configuration conf = new Configuration(true);//mv2
       Job job =new Job(conf,"DisplatSequence");//mv2
       job.setJarByClass(DisplaySequenceFile.class);//mv2
       
       SequenceFile.Reader theReader = new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
       
       int numrecords = 0;
       
       if (theReader.getValueClass() ==  BytesWritable.class)
       {
    	   Writable key = (Writable)(theReader.getKeyClass().newInstance());
    	   BytesWritable value = new BytesWritable();
    	   
    	   FastaRecord record = new FastaRecord();
    	   FileWriter fw = new FileWriter("/Users/mschatz/ref.br.txt");
    	   
    	   while(theReader.next(key,value))
    	   {
	    	   record.fromBytes(value);
	    	   fw.write(record.toString());
	    	   fw.write("\n");
           
	    	   numrecords++;
    	   }
           
    	
    	   fw.close();
       }
       else
       {
    	   Writable key = (Writable)(theReader.getKeyClass().newInstance());
    	   Writable value = (Writable)(theReader.getValueClass().newInstance());
       
    	   
    	   while(theReader.next(key,value))
    	   {
    		   System.out.println(key.toString() + " -> " + value.toString());
	    	   numrecords++;
    	   }
       }
       
       System.out.println("Saw " + numrecords);
   }

}
