package cloudBurst;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
//TODO migrate to yarn

public class PrintAlignments {
	
	private static JobConf conf = null;
	private static AlignmentRecord ar = new AlignmentRecord();
	
	public static void printFile(Path thePath,String directory) throws IOException
	{
		SequenceFile.Reader theReader = new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
	       
	    IntWritable key = new IntWritable();
	    BytesWritable value = new BytesWritable();
        try {
            System.out.println(thePath.getName() + " " + thePath.toString());
            File dir = new File(directory);
            File file = new File(dir,"results.txt");
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            while(theReader.next(key,value))
            {
                ar.fromBytes(value);
                output.write(ar.toAlignment(key.get()) + "\n");
            }
            output.close();
        } catch ( IOException e ) {
            e.printStackTrace();
        }

	}

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static int main(String[] args) throws IOException
	{
		String filename = null;
		//filename = "/user/guest/br-results/";
		
		if (filename == null)
		{
			if (args.length != 1) 
			{
				System.err.println("Usage: PrintAlignments seqfile");
				System.exit(-1);
			}
			
			filename = args[0];
		}
	
		
		System.err.println("Printing " + filename);
		
		Path thePath = new Path(filename);
	    conf = new JobConf(AlignmentStats.class);
	       
	    FileSystem fs = FileSystem.get(conf);
	       
	    if (!fs.exists(thePath))
	    {
	    	throw new IOException(thePath + " not found");   
	    }

	    FileStatus status = fs.getFileStatus(thePath);

	    if (status.isDir())
	    {    	   
	    	FileStatus[] files = fs.listStatus(thePath);
	    	for(FileStatus file : files)
	    	{
	    		String str = file.getPath().getName();

	    		if (str.startsWith("."))
	    		{
	    			// skip
	    		}			   
	    		else if (!file.isDir())
	    		{
	    			printFile(file.getPath(),filename);
	    		}
	    	}
	    }
	    else
	    {
	    	printFile(thePath,filename);
	    }
        return  1;
	}
}
