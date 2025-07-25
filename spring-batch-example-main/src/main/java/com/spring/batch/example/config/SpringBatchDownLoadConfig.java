package com.spring.batch.example.config;

import com.spring.batch.example.entity.Customer;
import com.spring.batch.example.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.logging.SimpleFormatter;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchDownLoadConfig {

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private CustomerRepository customerRepository;

    @Bean(name="downloadCSVJob")
    public Job downloadCSVJob() {
        return jobBuilderFactory.get("downloadCSVJob")
                .flow(step2()).end().build();

    }

    @Bean
    public RepositoryItemReader<Customer> reader2() {
        RepositoryItemReader<Customer> itemReader = new RepositoryItemReader<>();
        itemReader.setRepository(customerRepository);
        itemReader.setMethodName("findAll");
        final HashMap<String, Sort.Direction> sorts= new HashMap<>();
        sorts.put("id", Sort.Direction.ASC);
        itemReader.setSort(sorts);
        return itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;

    }

    @Bean
    public CustomerProcessor processor() {
        return new CustomerProcessor();
    }

    @Bean
    public FlatFileItemWriter<Customer> writer2() {

        Date now = new Date();
        String format1 = new SimpleDateFormat("yyyy-MM-dd'-'HH-mm-ss-SSSS", Locale.forLanguageTag("tr-TR")).format(now);
        Resource outputResource = new FileSystemResource("src/main/resources/output/user_"+ format1+".csv");

        String[] headers = new String[]{"id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob"};
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
        writer.setResource(outputResource);
        writer.setAppendAllowed(true);

        writer.setLineAggregator(new DelimitedLineAggregator<Customer>() {
            {
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<Customer>(){
                {
                    setNames(headers);
                }
            });
        }

        });

        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                for (int i = 0; i < headers.length; i++) {
                    if (i != headers.length -1) {
                        writer.append(headers[i] + ",");
                    }else{
                        writer.append(headers[i]);
                    }
                }
            }
        });

        return writer;
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("downloadCSVJobStep").<Customer, Customer>chunk(10)
                .reader(reader2())
                .processor(processor())
                .writer(writer2())
                .taskExecutor(taskExecutor())
                .build();
    }
    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }

}
