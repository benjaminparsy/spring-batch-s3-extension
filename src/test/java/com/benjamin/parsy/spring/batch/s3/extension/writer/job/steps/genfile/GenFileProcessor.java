package com.benjamin.parsy.spring.batch.s3.extension.writer.job.steps.genfile;

import com.benjamin.parsy.spring.batch.s3.extension.writer.job.steps.genfile.dto.ItemReadDto;
import com.benjamin.parsy.spring.batch.s3.extension.writer.job.steps.genfile.dto.ItemWriteDto;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;

public class GenFileProcessor implements ItemProcessor<ItemReadDto, ItemWriteDto> {

    @Override
    public ItemWriteDto process(@NonNull ItemReadDto item) {
        String line = String.format("%s|%s", item.id(), item.name());
        return new ItemWriteDto(line);
    }

}
