package com.chong.study.batch.chunk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.StopWatch;

import com.chong.study.Constans;
import com.chong.study.StudyApplication;

@SpringBatchTest
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = StudyApplication.class)
public class StudentJobTest {
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    Job job;

    @Test
    void testStudentJob() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        String filePath = "C:\\Users\\31238\\OneDrive\\デスクトップ\\test.csv";
        JobParameters jobParameters = new JobParametersBuilder().addString(Constans.FILE_PATH, filePath)
                .toJobParameters();
        jobLauncherTestUtils.getJobLauncher().run(job, jobParameters);
        stopWatch.stop();
        System.out.println(stopWatch.getTotalTimeMillis());
    }
}
