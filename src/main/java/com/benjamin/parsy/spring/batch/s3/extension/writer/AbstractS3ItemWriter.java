package com.benjamin.parsy.spring.batch.s3.extension.writer;

import io.awspring.cloud.s3.Location;
import io.awspring.cloud.s3.S3Exception;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractS3ItemWriter<T> extends AbstractItemStreamItemWriter<T>
        implements InitializingBean {

    public static final DataSize DEFAULT_BUFFER_CAPACITY = DataSize.ofMegabytes(5);

    private static final String PART_COUNTER = "part.counter";
    private static final String UPLOAD_ID = "upload.id";
    private static final String CURRENT_CONTENT = "current.content";
    private static final String COMPLETED_PART = "completed.part";

    private final S3Client s3Client;
    private final DataSize bufferSize;

    private ByteArrayOutputStream outputStream;

    private Location location;
    private S3HeaderCallback headerCallback;
    private S3FooterCallback footerCallback;

    private String uploadId;
    private int partCounter = 1;
    private List<CompletedPart> completedParts = new LinkedList<>();

    protected AbstractS3ItemWriter(S3Client s3Client, @Nullable DataSize bufferSize) {
        this.s3Client = s3Client;
        this.bufferSize = computeBufferSize(bufferSize);
        this.outputStream = new ByteArrayOutputStream((int) this.bufferSize.toBytes());
    }

    private static DataSize computeBufferSize(@Nullable DataSize bufferSize) {
        return bufferSize != null && bufferSize.toBytes() >= DEFAULT_BUFFER_CAPACITY.toBytes() ?
                bufferSize : DEFAULT_BUFFER_CAPACITY;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public void setHeaderCallback(S3HeaderCallback headerCallback) {
        this.headerCallback = headerCallback;
    }

    public void setFooterCallback(S3FooterCallback footerCallback) {
        this.footerCallback = footerCallback;
    }

    @Override
    public void write(@NonNull Chunk<? extends T> chunk) {

        byte[] content = doWrite(chunk);

        if (outputStream.size() >= bufferSize.toBytes()) {

            if (!isMultiPartUpload()) {
                createMultiPartUpload();
            }

            completedParts.add(uploadPart(outputStream.toByteArray(), uploadId));
            outputStream.reset();
        }

        try {
            outputStream.write(content);
        } catch (IOException e) {
            throw new WriteFailedException("Could not write data in buffer.", e);
        }

    }

    protected abstract byte[] doWrite(Chunk<? extends T> chunk);

    @SuppressWarnings("unchecked")
    @Override
    public void open(@NonNull ExecutionContext executionContext) {

        Assert.notNull(s3Client, "s3Client must not be null");
        Assert.notNull(location, "location must not be null");

        if (executionContext.containsKey(getExecutionContextKey(CURRENT_CONTENT))) {

            partCounter = executionContext.getInt(getExecutionContextKey(PART_COUNTER));
            uploadId = executionContext.getString(getExecutionContextKey(UPLOAD_ID));
            completedParts = (List<CompletedPart>) executionContext.get(getExecutionContextKey(COMPLETED_PART));

            byte[] bufferContent = executionContext.get(getExecutionContextKey(CURRENT_CONTENT), byte[].class);

            if (bufferContent != null) {
                outputStream.write(bufferContent, 0, bufferContent.length);
            }

        } else {

            try {
                outputStream.write(headerCallback.writeHeader());
            } catch (IOException e) {
                throw new WriteFailedException("Could not write header in buffer.", e);
            }

        }

    }

    @Override
    public void update(@NonNull ExecutionContext executionContext) {

        Assert.notNull(executionContext, "ExecutionContext must not be null");
        executionContext.putInt(getExecutionContextKey(PART_COUNTER), partCounter);
        executionContext.putString(getExecutionContextKey(UPLOAD_ID), uploadId);
        executionContext.put(getExecutionContextKey(CURRENT_CONTENT), outputStream.toByteArray());
        executionContext.put(getExecutionContextKey(COMPLETED_PART), completedParts);

    }

    @Override
    public void close() throws ItemStreamException {

        if (isClosed()) {
            return;
        }

        try {
            outputStream.write(footerCallback.writeFooter());
        } catch (IOException e) {
            throw new WriteFailedException("Could not write footer in buffer.", e);
        }

        if (isMultiPartUpload()) {
            completedParts.add(uploadPart(outputStream.toByteArray(), uploadId));
            completeMultiPartUpload(uploadId);
        } else {
            putObject(outputStream.toByteArray());
        }

        resetState();

    }

    private boolean isClosed() {
        return outputStream == null;
    }

    private boolean isMultiPartUpload() {
        return uploadId != null;
    }

    private void createMultiPartUpload() {

        try {
            uploadId = s3Client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                            .bucket(location.getBucket())
                            .key(location.getObject())
                            .build())
                    .uploadId();

        } catch (SdkException e) {
            throw new S3Exception("Failed to create multipart upload.", e);
        }
    }

    private CompletedPart uploadPart(byte[] content, String uploadId) {

        final UploadPartResponse response = s3Client.uploadPart(UploadPartRequest.builder()
                .bucket(location.getBucket())
                .key(location.getObject())
                .contentLength((long) content.length)
                .uploadId(uploadId)
                .partNumber(partCounter)
                .build(), RequestBody.fromBytes(content));

        return CompletedPart.builder()
                .partNumber(partCounter++)
                .eTag(response.eTag())
                .build();
    }

    private void completeMultiPartUpload(String uploadId) {

        try {
            s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(location.getBucket())
                    .key(location.getObject())
                    .uploadId(uploadId)
                    .multipartUpload(multipartUpload -> multipartUpload.parts(completedParts))
                    .build());

        } catch (SdkException e) {
            abortMultiPartUpload(uploadId);
            throw new S3Exception("Multipart upload failed.", e);
        }
    }

    private void abortMultiPartUpload(String uploadId) {

        try {
            s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(location.getBucket())
                    .key(location.getObject())
                    .uploadId(uploadId)
                    .build());

        } catch (SdkException e) {
            throw new S3Exception("Failed to abort the upload.", e);
        }
    }

    private void putObject(byte[] content) {

        try {
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(location.getBucket())
                    .key(location.getObject())
                    .contentLength((long) content.length)
                    .build(), RequestBody.fromBytes(content));

        } catch (SdkException e) {
            throw new S3Exception("Simple upload failed.", e);
        }
    }

    private void resetState() {
        this.uploadId = null;
        this.partCounter = 1;
        this.completedParts = new LinkedList<>();
        this.outputStream = null;
    }

}
