package com.benjamin.parsy.writer.s3.configuration;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.awspring.cloud.autoconfigure.core.AwsConnectionDetails;
import org.springframework.boot.testcontainers.service.connection.ContainerConnectionDetailsFactory;
import org.springframework.boot.testcontainers.service.connection.ContainerConnectionSource;
import org.springframework.lang.NonNull;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

public class AdobeS3MockConnectionDetailsFactory extends ContainerConnectionDetailsFactory<S3MockContainer, AwsConnectionDetails> {

    @Override
    protected AwsConnectionDetails getContainerConnectionDetails(ContainerConnectionSource<S3MockContainer> source) {
        return new AdobeS3MockContainerConnectionDetails(source);
    }

    private static final class AdobeS3MockContainerConnectionDetails extends ContainerConnectionDetails<S3MockContainer> implements AwsConnectionDetails {

        AdobeS3MockContainerConnectionDetails(ContainerConnectionSource<S3MockContainer> source) {
            super(source);
        }

        @Override
        public URI getEndpoint() {
            return URI.create(getContainer().getHttpEndpoint());
        }

        @Override
        public String getRegion() {
            return Region.US_EAST_1.id();
        }

        @NonNull
        @Override
        public String getAccessKey() {
            return "test";
        }

        @NonNull
        @Override
        public String getSecretKey() {
            return "test";
        }

    }

}
