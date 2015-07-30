package main;

import convertFile.ConvertFastaForCloud;

import java.io.IOException;

/**
 * Created by user on 7/27/15.
 */
public class mainConvert {

    public static void main(String[] args) {
        //qpath rpath outPath SampleName
        String[] params2 = new String[3];
        params2[0] = args[0];
        params2[1] = args[2] +"/qry.br";
        params2[2] = "Q";
        try {
            ConvertFastaForCloud.main(params2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        params2[0] = args[1];
        params2[1] = args[2] + "/ref.br";
        params2[2] = "R";
        try {
            System.out.println("start convert");
            ConvertFastaForCloud.main(params2);
            System.out.println("ConvertFastaForCloud.FastaList: " + ConvertFastaForCloud.FastaList.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
