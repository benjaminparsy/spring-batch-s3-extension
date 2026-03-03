package com.benjamin.parsy.spring.batch.s3.extension.writer;

import com.benjamin.parsy.spring.batch.s3.extension.writer.configuration.S3Extension;
import io.awspring.cloud.s3.Location;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.jspecify.annotations.NonNull;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

@Configuration
public class NominalJob {

    @Bean
    public Job job(JobRepository jobRepository,
                   Step step) {

        return new JobBuilder("job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository,
                     PlatformTransactionManager transactionManager,
                     FlatFileItemReader<ItemReadDto> reader,
                     ItemProcessor<ItemReadDto, ItemWriteDto> processor,
                     S3ItemWriter<ItemWriteDto> writer) {

        return new StepBuilder("step", jobRepository)
                .<ItemReadDto, ItemWriteDto>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<ItemReadDto> reader(@Value("#{jobParameters['filePath']}") String filePath) {

        return new FlatFileItemReaderBuilder<ItemReadDto>()
                .name(FlatFileItemReader.class.getSimpleName())
                .resource(new ClassPathResource(filePath))
                .linesToSkip(1)
                .lineTokenizer(new DelimitedLineTokenizer(","))
                .fieldSetMapper(fieldSet -> new ItemReadDto(
                        fieldSet.readRawString(0),
                        fieldSet.readRawString(1),
                        fieldSet.readRawString(2),
                        fieldSet.readRawString(3)
                ))
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<ItemReadDto, ItemWriteDto> process() {
        return item -> new ItemWriteDto(item.id(), item.firstname(), item.lastname(), item.email());
    }

    @Bean
    @StepScope
    public S3ItemWriter<ItemWriteDto> writer(S3Client s3Client, Converter<ItemWriteDto, byte[]> converter) {

        return new S3ItemWriterBuilder<ItemWriteDto>()
                .s3Client(s3Client)
                .bufferSize(DataSize.ofMegabytes(5))
                .location(Location.of(S3Extension.TEST_BUCKET, "file"))
                .byteConverter(converter)
                .headerCallback(() -> "<?xml version=\"1.0\"?><users>".getBytes(StandardCharsets.UTF_8))
                .footerCallback(() -> "</users>".getBytes(StandardCharsets.UTF_8))
                .build();
    }

    @Bean
    public Converter<ItemWriteDto, byte[]> converter() throws JAXBException {

        JAXBContext jaxbContext = JAXBContext.newInstance(ItemWriteDto.class);
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);

        return new Converter<>() {

            private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            @Override
            public byte[] convert(@NonNull ItemWriteDto source) {

                try {

                    marshaller.marshal(source, byteArrayOutputStream);
                    byte[] bytes = byteArrayOutputStream.toByteArray();
                    byteArrayOutputStream.reset();

                    return bytes;
                } catch (JAXBException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public record ItemReadDto(String id, String firstname, String lastname, String email) {

    }

    @XmlRootElement(name = "user")
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class ItemWriteDto {

        @XmlElement(name = "id")
        private String id;

        @XmlElement(name = "firstname")
        private String firstname;

        @XmlElement(name = "lastname")
        private String lastname;

        @XmlElement(name = "email")
        private String email;

        public ItemWriteDto() {
        }

        public ItemWriteDto(String id, String firstname, String lastname, String email) {
            this.id = id;
            this.firstname = firstname;
            this.lastname = lastname;
            this.email = email;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getFirstname() {
            return firstname;
        }

        public void setFirstname(String firstname) {
            this.firstname = firstname;
        }

        public String getLastname() {
            return lastname;
        }

        public void setLastname(String lastname) {
            this.lastname = lastname;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

    }

}
