/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wwh.exec;

import bgu.commons.exceptions.Exceptions;
import bgu.emc.wwh.catalog.model.MetaResource;
import bgu.emc.wwh.client.WWHClient;
import bgu.emc.wwh.mapreduce2.JobConfigurationProvider;
import bgu.emc.wwh.mapreduce2.WWHMapReduceJob;
import cloudBurst.MerReduce;
import createVector.VectorCreator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.Invoker;

/**
 *
 * @author bennyl
 */
public class Main {

    private static final String[] CLUSTERS = {"172.17.0.1", "172.17.0.2", "172.17.0.3"};
    private static final String MAIN_CLUSTER_RM = CLUSTERS[0];
    private static final String LOCAL_DATA_FOLDER = "data/";
    private static final String REMOTE_DATA_FOLDER = "/user/bennyl/bio/";
    private static final String LOCAL_WORKSPACE = LOCAL_DATA_FOLDER + "workspace";
    private static final String REMOTE_WORKSPACE = REMOTE_DATA_FOLDER + "workspace";
    private static final String CACHE_DB1_PATH = LOCAL_DATA_FOLDER + "cache/Cluster1DbIndex";
    private static final String CACHE_DB2_PATH = LOCAL_DATA_FOLDER + "cache/Cluster2DbIndex";
    private static final String CACHE_DB3_PATH = LOCAL_DATA_FOLDER + "cache/Cluster3DbIndex";
    private static final String OUTPUT_RESULT_PATH = REMOTE_DATA_FOLDER + "result";

    private static final String JAR_PATH = "target/one-jar.jar";
    private static final String POM_PATH = "pom.xml";

    private static final String CB_RESULT_PATH = "/user/bennyl/bio/cloud-burst-result";


    public static void main(String[] args) throws Exception {

        System.out.println("Prepering the environment... ");
        prepareEnvironment();

        System.out.println("Creating jobs... ");
        WWHClient client = WWHClient.create(createClusterConfiguration(MAIN_CLUSTER_RM));

        WWHMapReduceJob job = new WWHMapReduceJob("cloud-burst");

        job.setUnresolvedMetaResources("wwh.bio.outbreakmonitoring@c0",
                                       "wwh.bio.outbreakmonitoring@c1",
                                       "wwh.bio.outbreakmonitoring@c2");

        job.setLocalJobsConfigurationProvider(new LocalJobProvider());
        job.setGlobalJobsConfigurationProvider((j, resources) -> {
            j.setReducerClass(IdentityReducer.class);
            j.setOutputFormatClass(TextOutputFormat.class);
            return false;
        });

        job.setWorkspacePath(REMOTE_WORKSPACE);
        job.setOutputPath(OUTPUT_RESULT_PATH);

        client.submit(job, createJar()).waitForTermination();
        System.out.println("DONE!");
    }

    private static void prepareEnvironment() throws IOException {
        FileSystem lfs = FileSystem.get(new Configuration());

        for (int i = 0; i < CLUSTERS.length; i++) {
            YarnConfiguration configuration = createClusterConfiguration(CLUSTERS[i]);
            FileSystem fs = FileSystem.get(configuration);

            fs.delete(new Path(REMOTE_DATA_FOLDER), true);
            fs.delete(new Path("/user/bennyl/cloud-burst"), true);

            Path localData = new Path(LOCAL_DATA_FOLDER + "c" + i);

            FileUtil.copy(lfs, localData, fs, new Path(REMOTE_DATA_FOLDER), false, true, configuration);

        }

        final YarnConfiguration mainClusterConfiguration = createClusterConfiguration(MAIN_CLUSTER_RM);
        FileUtil.copy(new File(LOCAL_WORKSPACE), FileSystem.get(mainClusterConfiguration), new Path(REMOTE_DATA_FOLDER), false, mainClusterConfiguration);

    }

    private static String readFastaList(String CacheDbPath) throws IOException {
        String ans = "";
        BufferedReader br = new BufferedReader(new FileReader(CacheDbPath));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            if (line != null) {
                sb.append(line.split("\t")[0]);
                line = br.readLine();
            }
            while (line != null) {
                sb.append("$");
                sb.append(line.split("\t")[0]);
                line = br.readLine();
            }
            ans = sb.toString();
        } finally {
            br.close();
            System.out.println(ans);
            return ans;
        }
    }

    private static String readGeneLength(String CacheDbPath) throws IOException {
        String ans = "";
        BufferedReader br = new BufferedReader(new FileReader(CacheDbPath));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            ans = sb.toString();
        } finally {
            br.close();
            System.out.println(ans);
            return ans;
        }
    }

    private static YarnConfiguration createClusterConfiguration(String ip) {
        //create yarn configuration for c0 
        //A YarnConfiguration object is part of hadoop Yarn and 
        //is allows us to connect to a specific cluster and send application
        //masters into it.
        YarnConfiguration configuration = new YarnConfiguration();
        //configure the resource manager host name to submit jobs into
        configuration.set("yarn.resourcemanager.hostname", ip);
        //configure the hdfs to use
        configuration.set("fs.defaultFS", "hdfs://" + ip + ":9000");

//        configuration.set(RegistryConstants.KEY_REGISTRY_ZK_QUORUM, ip + ":2181");
        return configuration;
    }

    private static class LocalJobProvider implements JobConfigurationProvider {

        Map<String, String> cache;
        
        int numCall = 0;

        public LocalJobProvider() throws IOException {
            cache = new HashMap<>();
            
            cache.put("wwh.bio.outbreakmonitoring@c0#FASTA", readFastaList(CACHE_DB1_PATH));
            cache.put("wwh.bio.outbreakmonitoring@c1#FASTA", readFastaList(CACHE_DB2_PATH));
            cache.put("wwh.bio.outbreakmonitoring@c2#FASTA", readFastaList(CACHE_DB3_PATH));
            
            cache.put("wwh.bio.outbreakmonitoring@c0#GEN", readGeneLength(CACHE_DB1_PATH));
            cache.put("wwh.bio.outbreakmonitoring@c1#GEN", readGeneLength(CACHE_DB2_PATH));
            cache.put("wwh.bio.outbreakmonitoring@c2#GEN", readGeneLength(CACHE_DB3_PATH));
            
        }

        public boolean configureNext(Job job, MetaResource[] resources) throws IOException {

            Configuration conf = job.getConfiguration();

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            if (numCall == 0) {
                numCall++;
                conf.set("refpath", "ref.br");
                conf.set("qrypath", "qry.br");
                conf.set("MIN_READ_LEN", Integer.toString(99));
                conf.set("MAX_READ_LEN", Integer.toString(103));
                conf.set("K", Integer.toString(1));
                conf.set("SEED_LEN", Integer.toString(49));
                conf.set("FLANK_LEN", Integer.toString(55));
                conf.set("ALLOW_DIFFERENCES", Integer.toString(0));
                conf.set("BLOCK_SIZE", Integer.toString(128));
                conf.set("REDUNDANCY", Integer.toString(1));
                conf.set("FILTER_ALIGNMENTS", "0");
                conf.set("FastaList", cache.get(resources[0].getId().toString() + "#FASTA"));

                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                FileOutputFormat.setOutputPath(job, new Path(CB_RESULT_PATH));

                FileInputFormat.addInputPath(job, new Path(REMOTE_WORKSPACE + "/qry.br"));

                // The order of seeds is not important, but make sure the reference seeds are seen before the qry seeds
                job.setPartitionerClass(MerReduce.PartitionMers.class); // mv2
                job.setGroupingComparatorClass(MerReduce.GroupMersWC.class);

                job.setMapperClass(MerReduce.MapClass.class);
                job.setReducerClass(MerReduce.ReduceClass.class);
                job.setMapOutputKeyClass(BytesWritable.class);//mv2
                job.setMapOutputValueClass(BytesWritable.class);//mv2

                return true;
            } else {
                String clusterID = resources[0].getId().getClusterReferense();

                conf.set("GeneLengthLookup", cache.get(resources[0].getId().toString() + "#GEN"));
                conf.set("ClusterName", clusterID);

                FileInputFormat.setInputPaths(job, new Path(CB_RESULT_PATH));
                job.setMapperClass(VectorCreator.ClusterMapper.class);

                //conf.setInputFormat(SequenceFileInputFormat.class);
                job.setInputFormatClass(TextInputFormat.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setReducerClass(VectorCreator.ClusterReducer.class);

                return false;
            }

        }
    }

    public static File createJar() {

        System.out.println("Compiling project... ");
        Exceptions.uncheck(() -> {
            InvocationRequest request = new DefaultInvocationRequest();
            request.setPomFile(new File(POM_PATH));
            request.setGoals(Arrays.asList("package", "shade:shade"));

            Invoker invoker = new DefaultInvoker();
            System.out.println("executing!!!");
            invoker.execute(request);

        });

        return new File(JAR_PATH);
    }

    public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            while (values.iterator().hasNext()) {
                context.write(key, values.iterator().next());
            }
        }

    }
}
