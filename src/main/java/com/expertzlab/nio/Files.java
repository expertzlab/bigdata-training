package com.expertzlab.nio;

import java.io.*;

/**
 * Created by admin on 05/04/18.
 */
public class Files {

    public static void main(String[] args) throws IOException {

        Reader rd = new FileReader("input.txt");
        BufferedReader bfr = new BufferedReader(rd);

        Writer wr = new FileWriter("output.txt");
        BufferedWriter bfw = new BufferedWriter(wr);

        String str = null;

       while((str = bfr.readLine()) != null){
           bfw.write(str+"\n");
       }

       bfw.flush();
        //bfw.close();


    }
}
