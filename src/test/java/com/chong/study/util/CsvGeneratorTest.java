package com.chong.study.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StopWatch;

import com.chong.study.StudyApplication;
import com.chong.study.pojo.Student;

@SpringBootTest(classes = StudyApplication.class)
public class CsvGeneratorTest {

    @Test
    void TestGenerateCsv() throws IOException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        String filePath = "C:\\Users\\31238\\OneDrive\\デスクトップ\\test.csv";
        List<Student> list = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            list.add(StudentFactory.createStudent(i));
        }
        CsvGenerator.genatate(filePath, list);
        stopWatch.stop();
        System.out.println(stopWatch.getTotalTimeMillis());
    }
}
