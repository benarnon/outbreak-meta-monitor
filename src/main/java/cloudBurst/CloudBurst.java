package cloudBurst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

//TODO migrate to yarn -DONE
public class CloudBurst {
	
	// Make sure this number is longer than the longest read
	public static final int CHUNK_OVERLAP = 1024;

    public static void alignall(String refpath,
                                      String qrypath,
                                      String outpath,
                                      int MIN_READ_LEN,
                                      int MAX_READ_LEN,
                                      int K,
                                      int ALLOW_DIFFERENCES,
                                      boolean FILTER_ALIGNMENTS,
                                      int NUM_MAP_TASKS,
                                      int NUM_REDUCE_TASKS,
                                      int BLOCK_SIZE,
                                      int REDUNDANCY,
                                        String FastaList) throws IOException, Exception
	{


        int SEED_LEN   = MIN_READ_LEN / (K+1);
	int FLANK_LEN  = MAX_READ_LEN-SEED_LEN+K;

	Configuration conf = new Configuration(true);
        conf.set("refpath", "ref.br");
        conf.set("qrypath", "qry.br");
        conf.set("MIN_READ_LEN",      Integer.toString(MIN_READ_LEN));
        conf.set("MAX_READ_LEN",      Integer.toString(MAX_READ_LEN));
        conf.set("K",                 Integer.toString(K));
        conf.set("SEED_LEN",          Integer.toString(SEED_LEN));
        conf.set("FLANK_LEN",         Integer.toString(FLANK_LEN));
        conf.set("ALLOW_DIFFERENCES", Integer.toString(ALLOW_DIFFERENCES));
        conf.set("BLOCK_SIZE",        Integer.toString(BLOCK_SIZE));
        conf.set("REDUNDANCY",        Integer.toString(REDUNDANCY));
        conf.set("FILTER_ALIGNMENTS", (FILTER_ALIGNMENTS ? "1" : "0"));
        conf.set("FastaList",FastaList);

        Job job = new Job(conf, "cloudBurst");
        job.setNumReduceTasks(NUM_REDUCE_TASKS); // MV2

	FileInputFormat.addInputPath(job, new Path(refpath));
	FileInputFormat.addInputPath(job, new Path(qrypath));

        job.setJarByClass(MerReduce.class);//mv2

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // The order of seeds is not important, but make sure the reference seeds are seen before the qry seeds
	job.setPartitionerClass(MerReduce.PartitionMers.class); // mv2
	job.setGroupingComparatorClass(MerReduce.GroupMersWC.class);

        job.setMapperClass(MerReduce.MapClass.class);
        job.setReducerClass(MerReduce.ReduceClass.class);
        job.setMapOutputKeyClass(BytesWritable.class);//mv2
        job.setMapOutputValueClass(BytesWritable.class);//mv2
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path oPath = new Path(outpath);//TODO change it fit to the params
	FileOutputFormat.setOutputPath(job, oPath);
	System.err.println("  Removing old results");
	FileSystem.get(conf).delete(oPath);
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.err.println("Finished");
	}
	

	//------------------------- main --------------------------
	// Parse the command line options, run alignment and filtering
	public static void run(String[] args) throws  Exception{
        System.out.println("The args length: " + args.length);
        String refpath = null;
        String qrypath = null;
        String outpath = null;

        int K                = 0;
        int minreadlen       = 0;
        int maxreadlen       = 0;
        int allowdifferences = 0;

        int nummappers   = 1;
        int numreducers  = 1;
        int numfmappers  = 1;
        int numfreducers = 1;
        int blocksize    = 128;
        int redundancy   = 1;

        boolean filteralignments = false;
        String FastaList = "";


        if (args.length != 15)
        {
            System.err.println("Usage: CloudBurst refpath qrypath outpath minreadlen maxreadlen k allowdifferences filteralignments #mappers #reduces #fmappers #freducers blocksize redundancy");

            System.err.println();
            System.err.println("1.  refpath:          path in hdfs to the reference file");
            System.err.println("2.  qrypath:          path in hdfs to the query file");
            System.err.println("3.  outpath:          path to a directory to store the results (old results are automatically deleted)");
            System.err.println("4.  minreadlen:       minimum length of the reads");
            System.err.println("5.  maxreadlen:       maximum read length");
            System.err.println("6.  k:                number of mismatches / differences to allow (higher number requires more time)");
            System.err.println("7.  allowdifferences: 0: mismatches only, 1: indels as well");
            System.err.println("8.  filteralignments: 0: all alignments,  1: only report unambiguous best alignment (results identical to RMAP)");
            System.err.println("9.  #mappers:         number of mappers to use.              suggested: #processor-cores * 10");
            System.err.println("10. #reduces:         number of reducers to use.             suggested: #processor-cores * 2");
            System.err.println("11. #fmappers:        number of mappers for filtration alg.  suggested: #processor-cores");
            System.err.println("12. #freducers:       number of reducers for filtration alg. suggested: #processor-cores");
            System.err.println("13. blocksize:        number of qry and ref tuples to consider at a time in the reduce phase. suggested: 128");
            System.err.println("14. redundancy:       number of copies of low complexity seeds to use. suggested: # processor cores");

            return;
        }
        else
        {
            refpath          = args[0];
            qrypath          = args[1];
            outpath          = args[2];
            minreadlen       = Integer.parseInt(args[3]);
            maxreadlen       = Integer.parseInt(args[4]);
            K                = Integer.parseInt(args[5]);
            allowdifferences = Integer.parseInt(args[6]);
            filteralignments = Integer.parseInt(args[7]) == 1;
            nummappers       = Integer.parseInt(args[8]);
            numreducers      = Integer.parseInt(args[9]);
            numfmappers      = Integer.parseInt(args[10]);
            numfreducers     = Integer.parseInt(args[11]);
            blocksize        = Integer.parseInt(args[12]);
            redundancy       = Integer.parseInt(args[13]);
            FastaList        = args[14];
        }

        if (redundancy < 1) { System.err.println("minimum redundancy is 1"); return; }

        if (maxreadlen > CHUNK_OVERLAP)
        {
            System.err.println("Increase CHUNK_OVERLAP for " + maxreadlen + " length reads, and reconvert fasta file");
            return;
        }

        // start the timer
        Timer all = new Timer();

        String alignpath = outpath;
        // run the alignments
        Timer talign = new Timer();
        alignall(refpath, qrypath, alignpath, minreadlen, maxreadlen, K, allowdifferences, filteralignments,
                nummappers, numreducers, blocksize, redundancy,FastaList);
        System.err.println("Alignment time: " + talign.get());
        System.err.println("Total Running time:  " + all.get());

    }

}
