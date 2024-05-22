package com.chong.study.util;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.chong.study.StudyApplication;

@SpringBootTest(classes = StudyApplication.class)
public class StudentFactoryTest {

    @Test
    void testCreateStudent() {
        System.out.println(StudentFactory.createStudent(0));
    }
}
