package createHistograms;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

/**
 * Created by user on 7/28/15.
 */
public class HistogramParser {
    public static void parse(String inputFile,String outputPath) throws IOException {
        String lastGene = "";
        try {
            Path pt=new Path(inputFile);
            FileSystem fs = FileSystem.get(new Configuration());
            if (!fs.exists(pt))
                System.out.println("Input file not found");
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            File file;
            // garbage, only for the start
            FileWriter output = null;
            // end of garbage
            int regular_counter = 1;
            int complimentary_counter = 1;
            int lineNumber = 0;
            while((line = br.readLine()) != null){
                System.out.println("executing line " + (++lineNumber) );
                String clusterID = line.substring(0,line.indexOf("-"));
                String currGeneName = line.substring(line.indexOf("-") + 1, line.indexOf('\t'));
                currGeneName = currGeneName.replaceAll("\\|", "_");
                currGeneName = currGeneName.replaceAll(":", "_");
                currGeneName= currGeneName.replaceAll("-", "_");
                int start = Integer.parseInt(line.split("\t")[1]);
                int end = Integer.parseInt(line.split("\t")[2]);
                regular_counter = start;
                line = line.substring(line.indexOf("\t"),line.length());
                line =  line.substring(line.indexOf("+"),line.length());
                line =  line.substring(line.indexOf("|"),line.length());
                while(line.indexOf("$") == -1){
                    line = line + br.readLine();
                }
                String depth = line.split("\t")[2];
                String coverage = line.split("\t")[3];
                String integral = line.split("\t")[4];
                String numOfReads = line.split("\t")[5];
                String regularStrand = line.substring(0, line.indexOf("-"));
                String complimentaryStrand = line.substring(line.indexOf("-")+1,line.length());

                if(!currGeneName.equalsIgnoreCase(lastGene)){
                    if(lineNumber != 1) {
                        output.close();
                    }
                    File theDir = new File(outputPath +"/" +clusterID);

                    // if the directory does not exist, create it
                    if (!theDir.exists()) {
                        System.out.println("creating directory: " + outputPath +"/" +clusterID);
                        boolean result = false;

                        try{
                            theDir.mkdir();
                            result = true;
                        }
                        catch(SecurityException se){
                            //handle it
                        }
                        if(result) {
                            System.out.println("DIR created");
                        }
                    }

                    file = new File(outputPath +"/" +clusterID,  currGeneName + ".csv");
                    file.createNewFile();
                    output = new FileWriter(file);
                    output.append("Depth,Coverage,Integral,Number of reads\n");
                    output.append(depth +"," + coverage + "," + integral + "," + numOfReads +"\n");
                    output.append("position,depth,type_of_strand");
                    output.append('\n');
                    output.flush();
                }
                // append histogram to file

                //add regular strand

                int index = 0;
                while(regular_counter <= end && regularStrand.indexOf('|',index+2) != -1){
                    int next = regularStrand.indexOf('|',index+2);
                    String tuple = regularStrand.substring(index+1,next);
                    String range = tuple.split(",")[1];
                    String value = tuple.split(",")[2];
                    for (int i = 0; i < Integer.parseInt(range); i++) {
                        output.append(new Integer(i+regular_counter).toString());
                        output.append(",");
                        output.append(value);
                        output.append(",");
                        output.append("+");
                        output.append("\n");
                        output.flush();

                    }
                    regular_counter = regular_counter + Integer.parseInt(range);
                    index = next;

                }


                // adds the complimentary strand
                complimentary_counter = start;
                index=0;
                int next2 = complimentaryStrand.indexOf('|',index+2);
                while(complimentary_counter <= end  && complimentaryStrand.indexOf('|',index+2) != -1){

                    int next = complimentaryStrand.indexOf('|',index+2);
                    String tuple = complimentaryStrand.substring(index+1,next);
                    String range = tuple.split(",")[1];
                    String value = tuple.split(",")[2];
                    for (int i = 0; i < Integer.parseInt(range); i++) {
                        output.append(new Integer(i+complimentary_counter).toString());
                        output.append(",-");
                        output.append(value);
                        output.append(",");
                        output.append("-");
                        output.append("\n");
                        output.flush();
                    }
                    index = next+1;
                    complimentary_counter = complimentary_counter + Integer.parseInt(range);
                }

                lastGene = currGeneName;
            }

            output.close();



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("done");

    }
}
