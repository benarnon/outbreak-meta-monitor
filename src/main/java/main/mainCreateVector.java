package main;

import cloudBurst.CloudBurst;
import createHistograms.HistogramParser;
import createVector.VectorMapred;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by user on 7/6/15.
 */
public class mainCreateVector {
    static String SampleName;
    static String qpath;
    static String rpath;
    static String PathogenDbIndexPath;
    static String OutPath;
    static String healthyPath;

    static String maxlength;
    static String  minlength;
    static String CBk;//K for CloudBurst
    static String CBdiff;
    static String ShowResults;
    static String mapnum;
    static String reducenum;
    static String blocksize;
    static String redundancy;
    //General
    public static String[] params = new String[15];


    public static void RunVectorCreator() throws Exception {
        configue();
        String PathogenLength = toStringGeneLength();
        CloudBurst.run(params);
        VectorMapred.CreateVector(OutPath + "/" + SampleName +"/Local/Bed-File/" , OutPath + "/" + SampleName +"/Local/finalVecor/", 1 ,1,PathogenLength);
        HistogramParser.parse(OutPath + "/" + SampleName + "/" + "/Local/finalVecor/part-r-00000","/home/user/IdeaProjects/WWH-BioApp_resources/resources/Sanity_Resources/outputs");

    }

    private static void configue() throws IOException {
        params[0] = rpath;
        params[1] = qpath;
        params[2] = OutPath +"/"+ SampleName +"/Local/Bed-File/";

        params[3] = minlength;
        params[4] = maxlength;
        params[5] = CBk;
        params[6] = CBdiff;
        int filter = 0;
        params[7] = Integer.toString(filter);
        params[8] = mapnum;
        params[9] = reducenum;
        params[10] = "1";
        params[11] = "1";
        params[12] = blocksize;
        params[13] = redundancy;
        params[14] = toStringFastaList();
    }

    private static String toStringFastaList() throws IOException {
        String ans= "";
        BufferedReader br = new BufferedReader(new FileReader(PathogenDbIndexPath));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            if(line!= null){
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

    private static String toStringGeneLength() throws IOException {
        String ans= "";
        BufferedReader br = new BufferedReader(new FileReader(PathogenDbIndexPath));
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


    public static void main(String[] args) throws Exception {
        if(args.length != 15){
            System.out.println(args.length + " " + args[0]);
            System.out.println("SampleName SampleFilePath HealthySamplePath PathogenDbPath PathogenDbIndexFilePath OutputPath MaxReadLength MinReadLength K AllowDiff ShowResults NumOfMappers NumOfReducers BlockSize Redundancy");
            return;
        }
        SampleName =args[0];
        qpath = args[1];
        healthyPath = args[2];
        rpath = args[3];
        PathogenDbIndexPath = args[4];
        OutPath = args[5];

        maxlength = (args[6]);
        minlength = (args[7]);
        CBk = (args[8]);
        CBdiff = (args[9]);
        ShowResults = (args[10]);
        mapnum = (args[11]);
        reducenum = (args[12]);
        blocksize = (args[13]);
        redundancy = (args[14]);
        RunVectorCreator();


    }
}
