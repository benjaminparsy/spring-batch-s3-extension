package com.benjamin.parsy.spring.batch.s3.extension.writer.job.steps.genfile;

import com.benjamin.parsy.spring.batch.s3.extension.writer.S3FooterCallback;
import com.benjamin.parsy.spring.batch.s3.extension.writer.S3HeaderCallback;
import com.benjamin.parsy.spring.batch.s3.extension.writer.S3ItemWriter;
import com.benjamin.parsy.spring.batch.s3.extension.writer.S3ItemWriterBuilder;
import com.benjamin.parsy.spring.batch.s3.extension.writer.configuration.S3Extension;
import com.benjamin.parsy.spring.batch.s3.extension.writer.job.steps.genfile.dto.ItemWriteDto;
import io.awspring.cloud.s3.Location;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.charset.StandardCharsets;

@Component
@StepScope
public class GenFileWriterFactory {

    private final S3Client s3Client;

    public GenFileWriterFactory(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public S3ItemWriter<ItemWriteDto> createS3PartItemWriter() {

        return new S3ItemWriterBuilder<ItemWriteDto>()
                .s3Client(s3Client)
                .bufferSize(DataSize.ofMegabytes(5))
                .location(Location.of(S3Extension.TEST_BUCKET, "file"))
                .byteConverter(createConverter())
                .headerCallback(createHeaderCallback())
                .footerCallback(createFooterCallback())
                .build();
    }

    private Converter<Chunk<? extends ItemWriteDto>, byte[]> createConverter() {

        return source -> {

            String lines = "";
            for (ItemWriteDto itemWriteDto : source) {
                lines = lines.concat(itemWriteDto.line())
                        .concat("\n");
            }

            return lines.getBytes(StandardCharsets.UTF_8);
        };
    }

    private S3HeaderCallback createHeaderCallback() {
        return () -> "id|name\n".getBytes(StandardCharsets.UTF_8);
    }

    private S3FooterCallback createFooterCallback() {
        return () -> "end\n".getBytes(StandardCharsets.UTF_8);
    }

}
