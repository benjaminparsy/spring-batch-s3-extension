package com.benjamin.parsy.writer.s3.job.steps.genfile;

import com.benjamin.parsy.writer.s3.S3ItemWriter;
import com.benjamin.parsy.writer.s3.job.steps.genfile.dto.ItemReadDto;
import com.benjamin.parsy.writer.s3.job.steps.genfile.dto.ItemWriteDto;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.LinkedList;
import java.util.List;

@Configuration
public class GenFileStepConfig {

    @Bean
    public Step genFileStep(JobRepository jobRepository,
                            PlatformTransactionManager transactionManager,
                            ListItemReader<ItemReadDto> reader,
                            S3ItemWriter<ItemWriteDto> writer) {

        return new StepBuilder("genFileStep", jobRepository)
                .<ItemReadDto, ItemWriteDto>chunk(100, transactionManager)
                .reader(reader)
                .processor(new GenFileProcessor())
                .writer(writer)
                .build();
    }

    @Bean
    @StepScope
    public ListItemReader<ItemReadDto> reader() {

        List<ItemReadDto> list = new LinkedList<>();
        list.add(new ItemReadDto("1", "name1"));
        list.add(new ItemReadDto("2", "name2"));

        return new ListItemReader<>(list);
    }

    @Bean
    @StepScope
    public S3ItemWriter<ItemWriteDto> s3PartItemWriter(GenFileWriterFactory factory) {
        return factory.createS3PartItemWriter();
    }

}
