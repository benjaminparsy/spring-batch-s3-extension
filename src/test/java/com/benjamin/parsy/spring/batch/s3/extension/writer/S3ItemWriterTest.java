package com.benjamin.parsy.spring.batch.s3.extension.writer;

import com.benjamin.parsy.spring.batch.s3.extension.writer.configuration.S3Extension;
import com.benjamin.parsy.spring.batch.s3.extension.writer.configuration.TestConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
        classes = TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@ExtendWith(S3Extension.class)
@TestPropertySource(properties = {
        "spring.batch.jdbc.initialize-schema=always",
        "spring.batch.job.enabled=false",
        "spring.cloud.aws.s3.path-style-access-enabled=true"
})
class S3ItemWriterTest {

    private final JobLauncherTestUtils jobLauncherTestUtils;
    private final S3Client s3Client;

    @Autowired
    public S3ItemWriterTest(JobLauncherTestUtils jobLauncherTestUtils, S3Client s3Client) {
        this.jobLauncherTestUtils = jobLauncherTestUtils;
        this.s3Client = s3Client;
    }

    @Test
    void launchJob_withValidDate_shouldWriteInS3() {

        // Given
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("id", System.currentTimeMillis())
                .toJobParameters();

        // When
        JobExecution jobExecution = assertDoesNotThrow(() -> jobLauncherTestUtils.launchJob(jobParameters));

        // Then
        // Check batch status
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertTrue(jobExecution.getAllFailureExceptions().isEmpty());

        // Check S3 objects
        List<S3Object> s3ObjectList = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(S3Extension.TEST_BUCKET)
                        .build())
                .contents();

        assertEquals(1, s3ObjectList.size());

        // Check file
        ResponseBytes<GetObjectResponse> response =
                s3Client.getObject(GetObjectRequest.builder()
                        .key(s3ObjectList.getFirst().key())
                        .bucket(S3Extension.TEST_BUCKET)
                        .build(), ResponseTransformer.toBytes());

        String content = response.asString(StandardCharsets.UTF_8);
        assertEquals("""
                id|name
                1|name1
                2|name2
                end
                """, content);

    }

}