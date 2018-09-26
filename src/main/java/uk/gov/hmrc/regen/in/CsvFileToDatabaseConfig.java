package uk.gov.hmrc.regen.in;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;
import javax.validation.ValidationException;
import javax.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.UrlResource;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import uk.gov.hmrc.regen.common.ApplicationConfiguration;
import uk.gov.hmrc.regen.common.SourceContentDTO;

@EnableBatchProcessing
@Configuration
public class CsvFileToDatabaseConfig {
	private static final Logger log = LoggerFactory.getLogger(CsvFileToDatabaseConfig.class);

	@Autowired
	ApplicationConfiguration config;

	// Bean created automatically through Spring Batch libraries on the classpath.
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	// Bean created automatically through Spring Batch libraries on the classpath.
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	// 'spring.datasource' properties on the classpath, used to create the dataSource bean
	@Autowired
	private DataSource dataSource;
	
	@Autowired 
	private RecordValidationListener validationListener;

	/*
	 * The Spring Batch ItemReader for the step. This Spring batch extension is 'tuned' for the 
	 * reading of flat files.
	 */
	@Bean
	public ItemReader<SourceContentDTO> csvFileReader() throws MalformedURLException {
		FlatFileItemReader<SourceContentDTO> reader = new FlatFileItemReader<SourceContentDTO>();
		reader.setStrict(false); // Don't fail if file not there

		reader.setResource(new UrlResource(config.getPROCESSED_FILE()));

		// Use the SpringBatch DefaultLineMapper with default command field delimeter
		reader.setLineMapper(new DefaultLineMapper<SourceContentDTO>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "field1", "field2", "field3" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<SourceContentDTO>() {
					{
						setTargetType(SourceContentDTO.class);
					}
				});
			}
		});
		return reader;
	}

	
	/*
	 * Bean to expose the underlying validation framework (JSR352 implementation) - in our case - Hibernate.
	 */
    @Bean
    public Validator validatorFactory () {
        return new LocalValidatorFactoryBean();
    }
    
	/*
	 * The Spring Batch ItemProcessor for the step. The function to process the content simply
	 * adds extra text to each field.
	 */
	@Bean
	ItemProcessor<SourceContentDTO, SourceContentDTO> csvFileProcessor() {
		return (fileContentDTO) -> {
			final String field1 = "READ:" + fileContentDTO.getField1();
			final String field2 = "READ:" + fileContentDTO.getField2();
			final String field3 = "READ:" + fileContentDTO.getField3();

			final SourceContentDTO actualFCDTO = new SourceContentDTO(field1, field2, field3);

			log.debug("Converting (" + fileContentDTO + ") into (" + actualFCDTO + ")");

			return actualFCDTO;
		};
	}
	
	/*
	 * The Spring Batch ItemProcessor for the step that bundles together all the other processors. In this case, it's
	 * just one, but kept in just in case others are required in the future
	 */
	@Bean
	ItemProcessor<SourceContentDTO, SourceContentDTO> inputProcessor() {
		CompositeItemProcessor<SourceContentDTO, SourceContentDTO> processor = new CompositeItemProcessor<>();
		List<ItemProcessor<? super SourceContentDTO, ? super SourceContentDTO>> allProcessors = new ArrayList<>(2);
		allProcessors.add(csvFileProcessor());
		processor.setDelegates(allProcessors);
		return processor;
	}

	/*
	 * The Spring Batch ItemWriter for the step. This uploads the data read from the file into the database tables.
	 */
	@Bean
	public ItemWriter<SourceContentDTO> toDBWriter() {
		JdbcBatchItemWriter<SourceContentDTO> toDBWriter = new JdbcBatchItemWriter<SourceContentDTO>();
		toDBWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<SourceContentDTO>());
		toDBWriter.setSql("INSERT INTO FIELDS (field1, field2, field3, processed) VALUES (:field1, :field2, :field3, false)");
		toDBWriter.setDataSource(dataSource);
		return toDBWriter;
	}


	/*
	 * The actual step to be processed by the Job. Is an aggregation of the reader, processor and writer, as
	 * defined above, and these are injected as parameters.
	 * The step is chunked to allow volume commits/rollbacks and is made fault tolerant to allow failure where
	 * data validation occurs.
	 */
	@Bean
	public Step csvFileToDatabaseStep(
			@Qualifier("csvFileReader") ItemReader<SourceContentDTO> reader,
			@Qualifier("csvFileProcessor") ItemProcessor<SourceContentDTO,SourceContentDTO> processor,
			@Qualifier("toDBWriter") ItemWriter<SourceContentDTO> writer) throws Exception {

		return stepBuilderFactory.get("csvFileToDatabaseStep").allowStartIfComplete(true).
				<SourceContentDTO, SourceContentDTO> chunk(5).
				faultTolerant().noSkip(ValidationException.class).
				reader(reader).
				processor(processor).
				writer(writer).
				listener(validationListener).
				build();
	}

	/*
	 * The driving job, which executes the step, by default will be a re-startable job. Therefore, if a 
	 * non-completed instance is found, the previously failed job will try to run to completion.
	 */
	@Bean
	Job csvFileToDatabaseJob(@Qualifier("csvFileToDatabaseStep") Step step,
										FileReadCompletionListener listener) throws Exception {
		return jobBuilderFactory.get("csvFileToDatabaseJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(step).end().build();
	}
}
