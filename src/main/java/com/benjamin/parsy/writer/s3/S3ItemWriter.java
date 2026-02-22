package com.benjamin.parsy.writer.s3;

import org.springframework.batch.item.Chunk;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ItemWriter<T> extends AbstractS3ItemWriter<T> {

    protected Converter<Chunk<? extends T>, byte[]> byteConverter;

    protected S3ItemWriter(S3Client s3Client, @Nullable DataSize bufferSize) {
        super(s3Client, bufferSize);
        this.setName(ClassUtils.getShortName(S3ItemWriter.class));
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(byteConverter, "A byteConverter must be provided.");
    }

    public void byteConverter(Converter<Chunk<? extends T>, byte[]> byteConverter) {
        this.byteConverter = byteConverter;
    }

    @Override
    protected byte[] doWrite(Chunk<? extends T> chunk) {
        return byteConverter.convert(chunk);
    }

}
