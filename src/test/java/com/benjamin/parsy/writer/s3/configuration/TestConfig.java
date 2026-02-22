package com.benjamin.parsy.writer.s3.configuration;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@Configuration
@EnableAutoConfiguration
@ComponentScan(basePackages = "com.benjamin.parsy")
public class TestConfig {

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils(JobLauncher jobLauncher,
                                                     JobRepository jobRepository,
                                                     Job job) {

        JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJobRepository(jobRepository);
        jobLauncherTestUtils.setJob(job);
        jobLauncherTestUtils.setJobLauncher(jobLauncher);

        return jobLauncherTestUtils;
    }

    @Bean
    @ServiceConnection
    public PostgreSQLContainer postgreSQLContainer() {
        return new PostgreSQLContainer("postgres:17.5");
    }

    @Bean
    @ServiceConnection
    public S3MockContainer s3Container() {
        return new S3MockContainer(DockerImageName.parse("adobe/s3mock:4.11.0"));
    }

}
