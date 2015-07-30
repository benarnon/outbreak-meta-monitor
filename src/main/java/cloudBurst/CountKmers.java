package cloudBurst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

//TODO migrate to yarn - DONE


public class CountKmers {

		public static class MerMapClass extends Mapper<IntWritable, BytesWritable, BytesWritable, IntWritable>
		{
			private FastaRecord record = new FastaRecord();
			private BytesWritable mer = new BytesWritable();
			private IntWritable pos = new IntWritable(1);
			private byte [] dnabuffer = null;
			private int KMER_LEN;

			public void configure(Configuration conf)
			{
				KMER_LEN = Integer.parseInt(conf.get("KMER_LEN"));
				dnabuffer = new byte[DNAString.arrToDNALen(KMER_LEN)];
			}

			public void map(IntWritable id, BytesWritable rawRecord,Context context) throws IOException, InterruptedException {
				record.fromBytes(rawRecord);
				
				byte [] seq         = record.m_sequence;
				int realoffsetstart = record.m_offset;
				int seqlen = seq.length;
				
				int startoffset = 0;
				
				// If I'm not the first chunk, shift over so there is room for the left flank
				if (realoffsetstart != 0)
				{
					int shift = CloudBurst.CHUNK_OVERLAP + 1 - KMER_LEN;
					startoffset = shift;
					realoffsetstart += shift;
				}
				
				// stop so the last mer will just fit
				int end = seqlen - KMER_LEN + 1;
				
				for (int start = startoffset, realoffset = realoffsetstart; start < end; start++, realoffset++)
				{						
					if (DNAString.arrHasN(seq, start, KMER_LEN)) { continue; }
					DNAString.arrToDNAStr(seq, start, KMER_LEN, dnabuffer, 0);
					mer.set(dnabuffer, 0, dnabuffer.length);
					pos.set(realoffset);
					context.write(mer, pos);
				}
			}
		}
		
		
		public static class MerReduceClass extends Reducer<BytesWritable, IntWritable, Text, Text>
		{
			private static Text mertext = new Text();
			private static Text locations = new Text();
			private static StringBuilder builder = new StringBuilder();
			private boolean SHOW_POS;
			
			public void configure(Configuration conf)
			{
				SHOW_POS = (Integer.parseInt(conf.get("SHOW_POS")) == 0) ? false : true;
			}
					
			
			public synchronized void reduce(BytesWritable mer, Iterator<IntWritable> values,
					Context context)
                    throws IOException, InterruptedException {
				int cnt = 0;
				builder.setLength(0);
				
				while (values.hasNext()) 
				{
					cnt++;
					if (SHOW_POS)
					{
						builder.append('\t');
						builder.append(values.next().get());
					}
				}
				
				String val = DNAString.bytesToString(DNAString.bytesWritableDNAToArr(mer));
				mertext.set(val);
				
				if (SHOW_POS)
				{
					builder.insert(0, cnt);
					String locs = builder.toString();
					locations.set(locs);
				}
				else
				{
					locations.set(Integer.toString(cnt));
				}
				
				context.write(mertext, locations);
			}
		}
	

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inpath = null;
		String outpath = null;
		int kmerlen = 0;
		int numMappers = 1;
		int numReducers = 1;
		int showpos = 0;
		
		int data = 1;
		
		if (data == 0)
		{	
			if (args.length != 6)
			{
				System.err.println("Usage: CountKmers filename outpath kmerlen showpos numMappers numReducers");
				return;
			}
			
			inpath      =                  args[0];
			outpath     =                  args[1];
			kmerlen     = Integer.parseInt(args[2]);
			showpos     = Integer.parseInt(args[3]);
			numMappers  = Integer.parseInt(args[4]);
			numReducers = Integer.parseInt(args[5]);
		}
		else if (data == 1)
		{
			inpath = "/user/guest/cloudburst/s_suis.br";
			outpath = "/user/mschatz/kmers";
			kmerlen = 12;
			showpos = 0;
			numMappers = 1;
			numReducers = 1;
		}
		
		System.out.println("inpath: " + inpath);
		System.out.println("outpath: " + outpath);
		System.out.println("kmerlen: " + kmerlen);
		System.out.println("showpos: " + showpos);
		System.out.println("nummappers: " + numMappers);
		System.out.println("numreducers: " + numReducers);

        Configuration conf = new Configuration(true);
        Job job =new Job(conf,"CountMers");
        job.setJarByClass(MerReduce.class);
        //job.setNumMapTasks(numMappers);//TODO find solution for mv2
        job.setNumReduceTasks(numReducers);
		/*
        JobConf conf = new JobConf(MerReduce.class);
		conf.setNumMapTasks(numMappers);
		conf.setNumReduceTasks(numReducers);
        */
		FileInputFormat.addInputPath(job, new Path(inpath));
        conf.set("KMER_LEN", Integer.toString(kmerlen));
		conf.set("SHOW_POS", Integer.toString(showpos));

        job.setInputFormatClass(SequenceFileInputFormat.class);
		//conf.setInputFormat(SequenceFileInputFormat.class);

        job.setMapOutputKeyClass(BytesWritable.class);
		//conf.setMapOutputKeyClass(BytesWritable.class);

        job.setMapOutputValueClass(IntWritable.class);
		//conf.setMapOutputValueClass(IntWritable.class);
		//conf.setCompressMapOutput(true);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //conf.setOutputKeyClass(Text.class);
        //conf.setOutputValueClass(Text.class);
        //conf.setOutputFormat(TextOutputFormat.class);

        job.setMapperClass(MerMapClass.class);
        job.setReducerClass(MerReduceClass.class);

		//conf.setMapperClass(MerMapClass.class);
		//conf.setReducerClass(MerReduceClass.class);

		Path oPath = new Path(outpath);
        FileOutputFormat.setOutputPath(job, oPath);
		//FileOutputFormat.setOutputPath(conf, oPath);
		System.err.println("  Removing old results");
		FileSystem.get(conf).delete(oPath);
				

		Timer t = new Timer();
        int code = job.waitForCompletion(true) ? 0 : 1;
		System.err.println("CountMers Finished");
		
		System.err.println("Total Running time was " + t.get());
		
		//Counters counters = rj.getCounters( );TODO check if needed to move to mv2
		//Group task = counters.getGroup("org.apache.hadoop.mapred.Task$Counter");
		//long numDistinctMers = task.getCounter("REDUCE_INPUT_GROUPS");
		//System.err.println("Num Distinct Mers: " + numDistinctMers);
	}
}
