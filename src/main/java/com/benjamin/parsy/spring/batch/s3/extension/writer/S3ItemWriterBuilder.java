package com.benjamin.parsy.spring.batch.s3.extension.writer;

import io.awspring.cloud.s3.Location;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ItemWriterBuilder<T> {

    private S3Client s3Client;
    private DataSize bufferSize;
    private Location location;
    private Converter<T, byte[]> byteConverter;
    private S3HeaderCallback headerCallback;
    private S3FooterCallback footerCallback;

    public S3ItemWriterBuilder<T> s3Client(S3Client s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    public S3ItemWriterBuilder<T> bufferSize(DataSize bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public S3ItemWriterBuilder<T> location(Location location) {
        this.location = location;
        return this;
    }

    public S3ItemWriterBuilder<T> byteConverter(Converter<T, byte[]> byteConverter) {
        this.byteConverter = byteConverter;
        return this;
    }

    public S3ItemWriterBuilder<T> headerCallback(S3HeaderCallback headerCallback) {
        this.headerCallback = headerCallback;
        return this;
    }

    public S3ItemWriterBuilder<T> footerCallback(S3FooterCallback footerCallback) {
        this.footerCallback = footerCallback;
        return this;
    }

    public S3ItemWriter<T> build() {

        Assert.notNull(s3Client, "A s3Client is required.");
        Assert.notNull(location, "A location is required.");
        Assert.notNull(byteConverter, "A byteConverter is required.");

        S3ItemWriter<T> s3ItemWriter = new S3ItemWriter<>(s3Client, bufferSize, byteConverter);
        s3ItemWriter.setLocation(location);
        s3ItemWriter.setHeaderCallback(headerCallback);
        s3ItemWriter.setFooterCallback(footerCallback);

        return s3ItemWriter;
    }

}
