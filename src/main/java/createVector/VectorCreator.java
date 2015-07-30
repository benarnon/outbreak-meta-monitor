package createVector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VectorCreator {

    private static final Log LOG = LogFactory.getLog(VectorCreator.class);
    
    //Collecting all the contigs to one pathogen
    public static void CreateFinalVector(String vectorPath, List<String> DBlist, List<String> DBlength, String SampleName, String finalPath, int db) {
        //PathSum contain for each pathogen in the vector file the sum of all his contigs
        int[] PathSum = new int[DBlist.size()];

        BufferedReader br = null;
        try {

            br = new BufferedReader(new FileReader(vectorPath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            String line = null;
            if (br != null) {
                line = br.readLine();
            }

            while (line != null) {
                String id = line.split("\t")[0];
                int length = Integer.parseInt(line.split("\t")[1]);
                //if line is of pathogen with no contigs

                if (!line.contains("|gb|")) {
                    int in = DBlist.indexOf(id);
                    PathSum[in] = length;
                } //the pathogen is with contigs.
                else {
                    int indexCon = line.indexOf("|gb|");
                    //The 4 letters of the pathogen example: gi|158931597|gb|ABFK02000003.1| Alisti
                    String idCon = line.substring(indexCon + 4, indexCon + 8);
                    int iPatho = findPathogen(idCon, DBlist);
                    if (iPatho != -1) {
                        PathSum[iPatho] += length;
                    }
                }
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        PrintWriter writer = null;
        try {
            if (db == 0) {
                writer = new PrintWriter(finalPath + SampleName + "-final.txt", "UTF-8");
            } else if (db == 1) {
                writer = new PrintWriter(finalPath + SampleName + "-VectorEU.txt", "UTF-8");
            } else if (db == 2) {
                writer = new PrintWriter(finalPath + SampleName + "-VectorEgypt.txt", "UTF-8");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        if (writer != null) {
            if (db == 1) {
                writer.println("EU/" + SampleName);
            }
            if (db == 2) {
                writer.println("Egypt/" + SampleName);
            }
        }
        String line = "";
        for (int i = 0; i < DBlist.size(); i++) {
            line = DBlist.get(i) + "\t" + 100 * ((float) PathSum[i] / Float.parseFloat(DBlength.get(i)));
            if (writer != null) {
                writer.println(line);
            }
        }

        if (writer != null) {
            writer.close();
        }
    }

    private static int findPathogen(String idCon, List<String> dBlist) {
        for (int i = 0; i < dBlist.size(); i++) {
            if (dBlist.get(i).contains("|gb|")) {
                String tmp = dBlist.get(i);
                if (tmp.contains("|gb|" + idCon)) {
                    return i;
                }
            }
        }
        return -1;
    }

    public static class ClusterMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text alignmentData = new Text();
        private Text geneID = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {

                String sValue = value.toString();
                int splitPoint = sValue.indexOf('\t');
                geneID.set(sValue.substring(0, splitPoint));
                alignmentData.set(sValue.substring(splitPoint + 1));
                context.write(geneID, alignmentData);
            } catch (Exception e) {
                LOG.warn("ERROR WHEN PARSING: " + key + " => " + value);
                throw e;
            }
        }

    }

    public static class ClusterReducer extends Reducer<Text, Text, Text, Text> {

        private IntWritable result = new IntWritable();
        private Map<String, Integer> gene_size_lookup = new HashMap<String, Integer>();
        private AlignmentHistogram hist, reverseCompHist;
        private int geneUnitLength, numOfReads;
        private double depth, coverage;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            /*
             org.apache.hadoop.fs.Path[] cacheFiles = context.getLocalCacheFiles();
             BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
             try {
             StringBuilder sb = new StringBuilder();
             String line = br.readLine();

             while (line != null) {
             gene_size_lookup.put(line.split("\t")[0], Integer.parseInt(line.split("\t")[1]));
             line = br.readLine();
             }
             } finally {
             br.close();
             }
             */

            String geneLength = context.getConfiguration().get("GeneLengthLookup");
            String[] split = geneLength.split("\n");
            for (int i = 0; i < split.length; i++) {
                gene_size_lookup.put(split[i].split("\t")[0], Integer.parseInt(split[i].split("\t")[1]));
            }

        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] alignmentData;
            //System.out.println("THIS IS THE KEY!!!:" + key.toString());
            geneUnitLength = gene_size_lookup.get(key.toString().split(" ")[0]);
            hist = new AlignmentHistogram(geneUnitLength);
            //Size of histogram segments
            int jump = 100000;
            Iterator valIter = values.iterator();
            while (valIter.hasNext()) {

                alignmentData = valIter.next().toString().split("\t");
                int start = Integer.valueOf(alignmentData[0]);
                int end = Integer.valueOf(alignmentData[1]);
                int mismatches = Integer.valueOf(alignmentData[3]);
                boolean isComplemntry = false;
                double score = findScore(start, end, mismatches);

                if (alignmentData[4].equals("-")) {
                    isComplemntry = true;
                }
                hist.rangeAdd(start, end, score, isComplemntry);
            }
            double cov = hist.computeCoverage(1);
            double depth = hist.computeDepth();
            String value = cov + " " + depth;
            int location = 0;
            while (location < hist.getLength()) {
                if (hist.getLength() - location < jump) {
                    String tmp = context.getConfiguration().get("ClusterName") + "-" + key.toString();
                    Text newKey = new Text(tmp);
                    context.write(newKey, new Text(hist.toString(location, hist.getLength(), jump)));
                    location += jump;
                } else {
                    String tmp = context.getConfiguration().get("ClusterName") + "-" + key.toString();
                    Text newKey = new Text(tmp);
                    context.write(newKey, new Text(hist.toString(location, location + jump - 1, jump)));
                    location += jump;
                }

            }

        }

        private double findScore(int start, int end, int mismatches) {
            int alignmentLength = end - start;
            double score = 1 - mismatches / alignmentLength;
            return score;
        }
    }

}
