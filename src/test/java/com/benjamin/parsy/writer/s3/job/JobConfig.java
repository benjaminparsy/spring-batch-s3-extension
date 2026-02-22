package com.benjamin.parsy.writer.s3.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobConfig {

    @Bean
    public Job job(JobRepository jobRepository,
                   Step genFileStep) {

        return new JobBuilder("job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(genFileStep)
                .build();
    }

}