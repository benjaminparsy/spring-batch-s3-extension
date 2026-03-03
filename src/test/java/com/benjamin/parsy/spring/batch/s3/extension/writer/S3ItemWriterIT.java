package com.benjamin.parsy.spring.batch.s3.extension.writer;

import com.benjamin.parsy.spring.batch.s3.extension.writer.configuration.S3Extension;
import com.benjamin.parsy.spring.batch.s3.extension.writer.configuration.TestConfig;
import jakarta.xml.bind.JAXBException;
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
import org.springframework.core.convert.converter.Converter;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@ExtendWith(S3Extension.class)
class S3ItemWriterIT {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @MockitoSpyBean
    private S3Client s3Client;

    @MockitoSpyBean
    private Converter<NominalJob.ItemWriteDto, byte[]> converter;

    @Test
    void launchJob_withValidDate_shouldWriteInS3WithPutObject() {

        // Given
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("id", System.currentTimeMillis())
                .addString("filePath", "files/MOCK_DATA.csv")
                .toJobParameters();

        // When
        JobExecution jobExecution = assertDoesNotThrow(() -> jobLauncherTestUtils.launchJob(jobParameters));

        // Then
        // Check batch status
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertTrue(jobExecution.getAllFailureExceptions().isEmpty());
        jobExecution.getStepExecutions().forEach(s -> assertTrue(s.getFailureExceptions().isEmpty()));

        // Check method calls
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(s3Client, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3Client, never()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(s3Client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        // Check S3 objects
        List<S3Object> s3ObjectList = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(S3Extension.TEST_BUCKET)
                        .build())
                .contents();
        assertEquals(1, s3ObjectList.size());

        // Check file
        Document document = getDocument(GetObjectRequest.builder()
                .bucket(S3Extension.TEST_BUCKET)
                .key(s3ObjectList.getFirst().key())
                .build());

        assertEquals(1, document.getElementsByTagName("users").getLength());
        assertEquals(1000, document.getElementsByTagName("user").getLength());

    }

    @Test
    void launchJob_withValidDate_shouldWriteInS3WithMultipart() {

        // Given
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("id", System.currentTimeMillis())
                .addString("filePath", "files/MOCK_DATA_VOLUME.csv")
                .toJobParameters();

        // When
        JobExecution jobExecution = assertDoesNotThrow(() -> jobLauncherTestUtils.launchJob(jobParameters));

        // Then
        // Check batch status
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        assertTrue(jobExecution.getAllFailureExceptions().isEmpty());
        jobExecution.getStepExecutions().forEach(s -> assertTrue(s.getFailureExceptions().isEmpty()));

        // Check method calls
        verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(s3Client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3Client, times(3)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(s3Client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        // Check S3 objects
        List<S3Object> s3ObjectList = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(S3Extension.TEST_BUCKET)
                        .build())
                .contents();
        assertEquals(1, s3ObjectList.size());

        // Check file
        Document document = getDocument(GetObjectRequest.builder()
                .bucket(S3Extension.TEST_BUCKET)
                .key(s3ObjectList.getFirst().key())
                .build());

        assertEquals(1, document.getElementsByTagName("users").getLength());
        assertEquals(100000, document.getElementsByTagName("user").getLength());

    }

    @Test
    void launchJob_withRestart_shouldWriteInS3WithMultipart() {

        // Given
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("id", System.currentTimeMillis())
                .addString("filePath", "files/MOCK_DATA_VOLUME.csv")
                .toJobParameters();

        AtomicBoolean alreadyThrow = new AtomicBoolean(false);
        doAnswer(invocation -> {

            NominalJob.ItemWriteDto item = invocation.getArgument(0);

            if (!alreadyThrow.get() && item != null && "56017".equals(item.getId())) {
                alreadyThrow.set(true);
                throw new JAXBException("error");
            }

            return invocation.callRealMethod();
        }).when(converter).convert(any());

        JobExecution firstJobExecution = assertDoesNotThrow(() -> jobLauncherTestUtils.launchJob(jobParameters));
        assertEquals(BatchStatus.FAILED, firstJobExecution.getStatus());
        assertEquals(ExitStatus.FAILED.getExitCode(), firstJobExecution.getExitStatus().getExitCode());

        // When
        JobExecution secondJobExecution = assertDoesNotThrow(() -> jobLauncherTestUtils.launchJob(jobParameters));

        // Then
        // Check batch status
        assertEquals(BatchStatus.COMPLETED, secondJobExecution.getStatus());
        assertEquals(ExitStatus.COMPLETED, secondJobExecution.getExitStatus());
        assertTrue(secondJobExecution.getAllFailureExceptions().isEmpty());
        secondJobExecution.getStepExecutions().forEach(s -> assertTrue(s.getFailureExceptions().isEmpty()));

        // Check method calls
        verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(s3Client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3Client, times(3)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(s3Client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        // Check S3 objects
        List<S3Object> s3ObjectList = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(S3Extension.TEST_BUCKET)
                        .build())
                .contents();
        assertEquals(1, s3ObjectList.size());

        // Check file
        Document document = getDocument(GetObjectRequest.builder()
                .bucket(S3Extension.TEST_BUCKET)
                .key(s3ObjectList.getFirst().key())
                .build());

        assertEquals(1, document.getElementsByTagName("users").getLength());
        assertEquals(100000, document.getElementsByTagName("user").getLength());

    }

    @Test
    void launchJob_withRestart_shouldWriteInS3WithPutObject() {

        // Given
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("id", System.currentTimeMillis())
                .addString("filePath", "files/MOCK_DATA.csv")
                .toJobParameters();

        AtomicBoolean alreadyThrow = new AtomicBoolean(false);
        doAnswer(invocation -> {

            NominalJob.ItemWriteDto item = invocation.getArgument(0);

            if (!alreadyThrow.get() && item != null && "123".equals(item.getId())) {
                alreadyThrow.set(true);
                throw new JAXBException("error");
            }

            return invocation.callRealMethod();
        }).when(converter).convert(any());

        JobExecution firstJobExecution = assertDoesNotThrow(() -> jobLauncherTestUtils.launchJob(jobParameters));
        assertEquals(BatchStatus.FAILED, firstJobExecution.getStatus());
        assertEquals(ExitStatus.FAILED.getExitCode(), firstJobExecution.getExitStatus().getExitCode());

        // When
        JobExecution secondJobExecution = assertDoesNotThrow(() -> jobLauncherTestUtils.launchJob(jobParameters));

        // Then
        // Check batch status
        assertEquals(BatchStatus.COMPLETED, secondJobExecution.getStatus());
        assertEquals(ExitStatus.COMPLETED, secondJobExecution.getExitStatus());
        assertTrue(secondJobExecution.getAllFailureExceptions().isEmpty());
        secondJobExecution.getStepExecutions().forEach(s -> assertTrue(s.getFailureExceptions().isEmpty()));

        // Check method calls
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(s3Client, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3Client, never()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(s3Client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        // Check S3 objects
        List<S3Object> s3ObjectList = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(S3Extension.TEST_BUCKET)
                        .build())
                .contents();
        assertEquals(1, s3ObjectList.size());

        // Check file
        Document document = getDocument(GetObjectRequest.builder()
                .bucket(S3Extension.TEST_BUCKET)
                .key(s3ObjectList.getFirst().key())
                .build());

        assertEquals(1, document.getElementsByTagName("users").getLength());
        assertEquals(1000, document.getElementsByTagName("user").getLength());

    }

    private Document getDocument(GetObjectRequest getObjectRequest) {

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();

            return builder.parse(s3Client.getObject(getObjectRequest));
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new AssertionError(e);
        }

    }

}