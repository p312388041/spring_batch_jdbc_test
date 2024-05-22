package com.chong.study.batch.chunk;

import java.io.StringReader;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.chong.study.Constans;
import com.chong.study.pojo.Student;
import com.opencsv.CSVReader;

@Configuration
public class CsvToDbConfiguration {
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("job", jobRepository)
                .start(step(jobRepository, transactionManager))
                .build();
    }

    private Step step(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step", jobRepository)
                .<Student, Student>chunk(100, transactionManager)
                .reader(new StudentItemReader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    private JdbcBatchItemWriter<Student> writer() {
        String sql = "insert into student values(:id, :name, :chinese, :english, :math, :total)";
        JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<>();
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        writer.setJdbcTemplate(jdbcTemplate);
        writer.setSql(sql);
        writer.setItemPreparedStatementSetter((item, ps) -> {
            ps.setInt(1, item.getId());
            ps.setString(2, item.getName());
            ps.setFloat(3, item.getChinese());
            ps.setFloat(4, item.getEnglish());
            ps.setFloat(5, item.getMath());
            ps.setFloat(6, item.getTotal());
        });
        return writer;
    }

    private ItemProcessor<Student, Student> processor() {
        return (inStudent -> {
            Student outStudent = new Student(inStudent.getId(), inStudent.getName(), inStudent.getChinese(),
                    inStudent.getEnglish(), inStudent.getMath());
            outStudent.setTotal(inStudent.getChinese() + inStudent.getEnglish() + inStudent.getMath());
            return outStudent;
        });
    }

    private static class StudentItemReader extends FlatFileItemReader<Student> {
        private JobParameters jobParameters;

        @BeforeStep
        public void beforeStep(StepExecution stepExecution) {
            jobParameters = stepExecution.getJobParameters();
            setResource(new FileSystemResource(jobParameters.getString(Constans.FILE_PATH)));
            setLineMapper(studentLineMapper());
        }

        private LineMapper<Student> studentLineMapper() {
            return (line, lineNumber) -> {
                CSVReader csvReader = new CSVReader(new StringReader(line));
                String[] fileds = csvReader.readAll().get(0);
                csvReader.close();
                float chinese = Float.parseFloat(fileds[2]);
                float english = Float.parseFloat(fileds[3]);
                float math = Float.parseFloat(fileds[4]);
                return new Student(Integer.parseInt(fileds[0]), fileds[1], chinese, english, math);
            };
        }
    }
}