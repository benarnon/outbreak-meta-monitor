package convertFile;

import cloudBurst.DNAString;
import cloudBurst.FastaRecord;
import cloudBurst.CloudBurst;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import java.io.*;

//TODO migrate to yarn- DONE


public class ConvertFastaForCloud {

    public static String[] FastaList;


	private static final FastaRecord record = new FastaRecord();
	
	public static int min_seq_len = Integer.MAX_VALUE;
	public static int max_seq_len = 0;
		
	public static int min(int a, int b)
	{
		if (a < b) return a;
		return b;
	}
	
	public static int max(int a, int b)
	{
		if (a > b) return a;
		return b;
	}
		
	private static IntWritable iw = new IntWritable();
	
	public static void saveSequence(int id, StringBuilder sequence, Writer writer) throws IOException
	{	
		int fulllength = sequence.length();
		int maxchunk = 65535;
		
		if (fulllength < min_seq_len) { min_seq_len = fulllength; }
		if (fulllength > max_seq_len) { max_seq_len = fulllength; }
		
		if (fulllength > 100)
		{
			System.out.println("In " + id + "... "  + fulllength + "bp");
		}
		
		int offset = 0;
		int numchunks = 0;
		
		while(offset < fulllength)
		{
			numchunks++;
			int end = min(offset + maxchunk, fulllength);
			
			boolean lastChunk = (end == fulllength);

			record.m_sequence = DNAString.stringToBytes(sequence.substring(offset, end));
			record.m_offset = offset;
			record.m_lastChunk = lastChunk;
			
			iw.set(id);
			writer.append(iw, record.toBytes());
			
			if (end == fulllength) 
			{ 
				offset = fulllength; 
			}
			else
			{
				offset = end - CloudBurst.CHUNK_OVERLAP;
			}
		}
		
		if (numchunks > 1)
		{
			System.out.println("  " + numchunks + " chunks");
		}
	}
	
	public static void convertFile(String infile, Writer writer, String refQry) throws IOException
	{
		FastaList = new String[1000];
        String header = "";
		StringBuilder sequence = null;
		
		int count = 0;
		
		try
		{
            System.out.println("new-  " +infile);
            FileInputStream a = new FileInputStream(infile);
            //System.out.println(a.read());
            InputStreamReader b = new InputStreamReader(a);
            //System.out.println(b.read());
            BufferedReader data = new BufferedReader(b);
            String mapfile = infile;

            mapfile += ".map";
			FileWriter fstream = new FileWriter(mapfile);
		    BufferedWriter out = new BufferedWriter(fstream);

			String line;
			while ((line = data.readLine()) != null) 
			{
				line.trim();
				if (line.isEmpty())
				{
					// Guard against empty lines
					continue;
				}
				if (line.charAt(0) == '>')
				{
                    int help = refQry.compareTo("R");
                    if(refQry.compareTo("R") == 0)
                        FastaList[count] = line.split(">")[1];
					if (count > 0)
					{
					  saveSequence(count, sequence, writer);
					}
					
					sequence = new StringBuilder();
					header = line.substring(1); // skip the >
					count++;
					out.write(count + " " + header + "\n");
				}
				else
				{

					sequence.append(line.toUpperCase());
				}
			}
            saveSequence(count, sequence, writer);
			
		    out.close();
		} 
		catch (FileNotFoundException e) 
		{
			System.err.println("Can't open " + infile);
			e.printStackTrace();
			System.exit(1);
		}
		
		System.err.println("Processed " + count + " sequences");
	}
	
	
	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException 
	{
		if (args.length != 3) {
			System.err.println("Usage: ConvertFastaForCloud file.fa outfile.br");
			System.exit(-1);
		}
		
		String infile = args[0];
		String outfile = args[1];
        String RefQry = args[2];
		
		System.err.println("Converting " + infile + " into " + outfile);
		
		Configuration config = new Configuration(true);
		
		Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
                new Path(outfile), IntWritable.class, BytesWritable.class);
		
		convertFile(infile, writer,RefQry);
		
		writer.close();
		
		System.err.println("min_seq_len: " + min_seq_len);
		System.err.println("max_seq_len: " + max_seq_len);
		System.err.println("Using DNAString version: " + DNAString.VERSION);
	}
};
