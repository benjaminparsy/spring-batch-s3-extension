package com.benjamin.parsy.spring.batch.s3.extension.writer;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class S3ItemWriter<T> extends AbstractS3ItemWriter<T> {

    private final Converter<T, byte[]> byteConverter;

    public S3ItemWriter(S3Client s3Client, @Nullable DataSize bufferSize, Converter<T, byte[]> byteConverter) {
        super(s3Client, bufferSize);
        this.byteConverter = byteConverter;
        this.setName(ClassUtils.getShortName(S3ItemWriter.class));
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(byteConverter, "A byteConverter must be provided.");
    }

    @Override
    protected byte[] doWrite(Chunk<? extends T> items) {

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            for (T item : items) {
                byte[] bytes = byteConverter.convert(item);
                Assert.notNull(bytes, "Converter returned null for item: " + item);
                outputStream.write(bytes);
            }

            return outputStream.toByteArray();

        } catch (IOException e) {
            throw new WriteFailedException("Failed to write bytes", e);
        }
    }

}
