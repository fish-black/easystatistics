package com.fishblack.statistics.cardinality;

import com.fishblack.fastparquet.common.TempFile;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

public class FileCardinalityCalculator implements CardinalityCalculator {

    private TempFile carFile;
    private FileOutputStream fos;
    private final String LINE_BREAK = "\r\n";
    private long rowNumber = 0;
    private final String SPLIT_FILE_NAME_PRE = "/dss.part";
    private final String COMBINED_FILE_NAME_PRE = "/dss.c";
    private final int MAX_FILE_ROWS = 100000;

    public FileCardinalityCalculator() throws IOException {
        carFile = new TempFile();
        fos = new FileOutputStream(carFile.getPath(), true);
    }

    @Override
    public void add(Object data) throws IOException{
        fos.write(String.valueOf(data).getBytes());
        fos.write(LINE_BREAK.getBytes());
        rowNumber++;
    }

    @Override
    public long count() throws IOException{
        fos.close();
        File tempFile = new File(carFile.getPath());
        String workDir = tempFile.getParent() + "/" + new Date().getTime();
        File randomWorkDir = new File(workDir);
        if (!randomWorkDir.exists()){
            randomWorkDir.mkdirs();
        }
        int fileCount = (int) Math.ceil((double)rowNumber / MAX_FILE_ROWS);
        splitFile(carFile.getPath(), workDir, fileCount);
        sortFiles(workDir, fileCount);
        String totalOrderedFile = combineAllSplitFiles(workDir, fileCount);
        return getCardinality(totalOrderedFile);
    }

    private void splitFile(String sourceFilePath, String workDir,
                                 int fileCount) throws IOException {
        FileWriter fw = null;
        BufferedWriter bw = null;
        FileReader fr = new FileReader(sourceFilePath);
        BufferedReader br = new BufferedReader(fr);

        int i = 1;
        LinkedList fwLists=new LinkedList();
        LinkedList bwLists=new LinkedList();
        for (int j = 1; j <= fileCount; j++) {
            fw = new FileWriter(workDir + SPLIT_FILE_NAME_PRE + j ,false);
            bw=new BufferedWriter(fw);

            fwLists.add(fw);
            bwLists.add(bw);
        }
        while (br.ready()) {

            int count=1;
            for (Iterator iterator = bwLists.iterator(); iterator.hasNext();) {
                BufferedWriter type = (BufferedWriter) iterator.next();
                if(i==count){
                    type.write(br.readLine() + LINE_BREAK);
                    break;
                }
                count++;
            }
            if (i >= fileCount) {
                i = 1;
            } else
                i++;
        }
        br.close();
        fr.close();
        for (Iterator iterator = bwLists.iterator(); iterator.hasNext();) {
            BufferedWriter object = (BufferedWriter) iterator.next();
            object.close();
        }
        for (Iterator iterator = fwLists.iterator(); iterator.hasNext();) {
            FileWriter object = (FileWriter) iterator.next();
            object.close();
        }
    }

    private void sortFiles(String workDir, int fileCount) throws IOException {
        LinkedList values = null;
        for (int i = 1; i <= fileCount; i++) {
            values = new LinkedList();
            String path = workDir + SPLIT_FILE_NAME_PRE + i;
            try {
                FileReader fr = new FileReader(path);
                BufferedReader br = new BufferedReader(fr);
                while (br.ready()) {
                    values.add(br.readLine());
                }
                Collections.sort(values);
                writeBackSortedValues(values, path);

                br.close();
                fr.close();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeBackSortedValues(LinkedList list, String path) {
        try {
            FileWriter fs = new FileWriter(path);
            BufferedWriter fw=new BufferedWriter(fs);
            for (Iterator iterator = list.iterator(); iterator.hasNext();) {
                Object object = (Object) iterator.next();
                fw.write(object + LINE_BREAK);
            }
            fw.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String combineTwoOrderedFiles(String fileAPath, String fileBPath, String workDirPath) throws IOException{
        File workDir = new File(workDirPath);
        if (!workDir.exists()){
            workDir.mkdirs();
        }
        int i = 1;
        File combinedFile = new File(workDir + COMBINED_FILE_NAME_PRE + i);
        while (!combinedFile.createNewFile()) {
            i ++;
            combinedFile = new File(workDir + COMBINED_FILE_NAME_PRE + i);
        }
        FileWriter fw = null;
        BufferedWriter bw = null;
        FileReader frA = new FileReader(fileAPath);
        BufferedReader brA = new BufferedReader(frA);
        FileReader frB = new FileReader(fileBPath);
        BufferedReader brB = new BufferedReader(frB);

        fw = new FileWriter(combinedFile);
        bw = new BufferedWriter(fw);


        String A = brA.readLine();
        String B = brB.readLine();
        while(true) {
            if (A != null && B != null) {
                if (A.compareTo(B) < 0) {
                    bw.write(A + LINE_BREAK);
                    A = brA.readLine();
                } else if (A.compareTo(B) > 0) {
                    bw.write(B + LINE_BREAK);
                    B = brB.readLine();
                } else {
                    bw.write(A + LINE_BREAK);
                    bw.write(B + LINE_BREAK);
                    A = brA.readLine();
                    B = brB.readLine();
                }
            }
            else if(A == null){
                brA.close();
                frA.close();
                while( B!= null){
                    bw.write(B + LINE_BREAK);
                    B = brB.readLine();
                }
                break;
            }
            else if ( B == null){
                brB.close();
                frB.close();
                while( A!= null){
                    bw.write(A + LINE_BREAK);
                    A = brA.readLine();
                }
                break;
            }
        }
        bw.close();
        fw.close();
        FileUtils.forceDelete(new File(fileAPath));
        FileUtils.forceDelete(new File(fileBPath));
        return combinedFile.getPath();
    }

    private long getCardinality(String filePath){
        long ret = 0L;
        if (filePath == null || !new File(filePath).exists()){
            return ret;
        }
        try {
            FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr);
            String value = null;
            while (br.ready()) {
                String current = br.readLine();
                if (!current.equals(value)){
                    value = current;
                    ret ++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    private String combineAllSplitFiles(String workDir, int fileCount) throws IOException{
        String startA = workDir+ SPLIT_FILE_NAME_PRE + "1";
        String startB = workDir+ SPLIT_FILE_NAME_PRE + "2";
        if (new File(startA).exists()){
            if (new File(startB).exists()){
                String output = combineTwoOrderedFiles(startA, startB, workDir);
                for (int j = 3; j<=fileCount; j++){
                    String next = workDir+ SPLIT_FILE_NAME_PRE + j;
                    output = combineTwoOrderedFiles(output, next, workDir);
                }
                return output;
            }
            else{
                return startA;
            }
        }
        else {
            return null;
        }
    }


}
