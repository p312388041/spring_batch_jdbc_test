package com.chong.study.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import com.chong.study.pojo.Student;
import com.opencsv.CSVWriter;

public class CsvGenerator {
    public static void genatate(String filePath, List<Student> list) throws IOException {
        Writer writer = new FileWriter(filePath);
        CSVWriter csvWriter = new CSVWriter(writer);
        List<String []> lineList = new ArrayList<>();
        for (Student  student : list) {
            String [] line = new String[6];
            line[0] = student.getId() + "";
            line[1] = student.getName() + "";
            line[2] = student.getChinese() + "";
            line[3] = student.getEnglish() + "";
            line[4] = student.getMath() + "";
            line[5] = student.getTotal() + "";
            lineList.add(line);
        }
        csvWriter.writeAll(lineList);
        csvWriter.close();
    }
}
