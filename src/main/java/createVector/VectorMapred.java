package createVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by user on 3/1/15.
 */


public class VectorMapred {

    public static void CreateVector(String CBresults,
                                    String outpath,
                                    int nummappers,
                                    int numreducers,
                                    String GeneLengthLookup) throws IOException, Exception

    {
        String clusterID = "C5";
        System.out.println("NUM_FMAP_TASKS: "     + nummappers);
        System.out.println("NUM_FREDUCE_TASKS: "  + numreducers);

        Configuration conf = new Configuration(true);
        conf.set("GeneLengthLookup",GeneLengthLookup);
        conf.set("ClusterName",clusterID);

        Job job = new Job(conf, "createVector");
        //DistributedCache.addCacheFile(new URI("/Dist/gene_length_lookup.txt"),job.getConfiguration());

        job.setNumReduceTasks(numreducers); // MV2

        FileInputFormat.addInputPath(job, new Path(CBresults));
        job.setJarByClass(VectorCreator.class);
        job.setMapperClass(VectorCreator.ClusterMapper.class);

        //conf.setInputFormat(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(VectorCreator.ClusterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //conf.setOutputFormat(SequenceFileOutputFormat.class);


        Path oPath = new Path(outpath);
        FileOutputFormat.setOutputPath(job, oPath);
        //conf.setOutputPath(oPath);
        System.err.println("  Removing old results");
        FileSystem.get(conf).delete(oPath);


        //JobClient rj = JobClient.runJob(conf);
        int code = job.waitForCompletion(true) ? 0 : 1;

        System.err.println("Create Vector Finished");
    }

    public static void main(String[] args) throws Exception {
        try {
            CreateVector(args[0] , args[1] , Integer.valueOf(args[2]) , Integer.valueOf(args[3]),args[4]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}