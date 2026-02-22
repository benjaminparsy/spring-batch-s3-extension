package com.benjamin.parsy.writer.s3.configuration;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.List;
import java.util.Optional;

public class S3Extension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback {

    public static final String TEST_BUCKET = "bucket";

    @Override
    public void beforeAll(ExtensionContext context) {

        ApplicationContext ctx = SpringExtension.getApplicationContext(context);
        S3Client s3Client = ctx.getBean(S3Client.class);

        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(TEST_BUCKET)
                .build());
    }

    @Override
    public void beforeEach(ExtensionContext context) {

        ApplicationContext ctx = SpringExtension.getApplicationContext(context);
        S3Client s3Client = ctx.getBean(S3Client.class);

        deleteS3Objects(s3Client);

    }

    private void deleteS3Objects(S3Client s3Client) {

        List<ObjectIdentifier> objects = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(S3Extension.TEST_BUCKET)
                        .build())
                .contents()
                .stream()
                .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                .toList();

        if (!objects.isEmpty()) {
            s3Client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(S3Extension.TEST_BUCKET)
                    .delete(Delete.builder().objects(objects).build())
                    .build());
        }

    }

    @Override
    public void afterAll(ExtensionContext context) {

        ApplicationContext ctx = SpringExtension.getApplicationContext(context);
        S3Client s3Client = ctx.getBean(S3Client.class);

        Optional<String> bucket = s3Client.listBuckets()
                .buckets()
                .stream()
                .map(Bucket::name)
                .filter(TEST_BUCKET::equals)
                .findFirst();

        if (bucket.isEmpty()) {
            throw new IllegalArgumentException("Bucket doesn't exist");
        }

        deleteS3Objects(s3Client);

        s3Client.deleteBucket(DeleteBucketRequest.builder()
                .bucket(bucket.get())
                .build());
    }

}
