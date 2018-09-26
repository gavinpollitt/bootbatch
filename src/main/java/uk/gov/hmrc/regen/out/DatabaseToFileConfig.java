package uk.gov.hmrc.regen.out;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

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
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import uk.gov.hmrc.regen.common.ApplicationConfiguration;
import uk.gov.hmrc.regen.common.SourceContentDTO;

/**
 * 
 * @author GP
 *
 * Construct Job to produce the output file and update the database accordingly
 */
@EnableBatchProcessing
@Configuration
public class DatabaseToFileConfig {
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
	

	/*
	 * The Spring Batch ItemReader for the step. This Spring batch extension is 'tuned' for the 
	 * reading of database tables.
	 */
	@Bean
	ItemReader<SourceContentDTO> dbItemReader() {
		JdbcCursorItemReader<SourceContentDTO> databaseReader = new JdbcCursorItemReader<>();

		databaseReader.setDataSource(dataSource);
		databaseReader.setSql("SELECT field1, field2, field3 FROM fields WHERE processed is not true");
		databaseReader.setRowMapper(new BeanPropertyRowMapper<>(SourceContentDTO.class));

		return databaseReader;
	}

	/*
	 * The Spring Batch ItemProcessor for the step. Nothing needs to be done with the data
	 * in this case apart from return what was provided.
	 */
	@Bean
	ItemProcessor<SourceContentDTO, SourceContentDTO> dbContentProcessor() {
		return (dbContentDTO) -> dbContentDTO;
	}

	/*
	 * Returns an FieldExtractor instance that will take a bean object and extract the actual
	 * values into a collection
	 */
	private FieldExtractor<SourceContentDTO> createSourceFieldExtractor() {
		BeanWrapperFieldExtractor<SourceContentDTO> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] { "field1", "field2", "field3" });
		return extractor;
	}

	/*
	 * Utilising the extractor create an output based on the extractor values and the format provided.
	 */
	private LineAggregator<SourceContentDTO> createSourceLineAggregator() {
		FormatterLineAggregator<SourceContentDTO> lineAggregator = new FormatterLineAggregator<>();

		FieldExtractor<SourceContentDTO> fieldExtractor = createSourceFieldExtractor();
		lineAggregator.setFieldExtractor(fieldExtractor);
		lineAggregator.setFormat("F1:%-10s*F2:%-10s*F3%-10s");

		return lineAggregator;
	}

	
	/*
	 * One of the Spring Batch ItemWriters for the step. This one is responsible for producing the file output
	 * utilising the aggregator output. Will only produce a file if at least one record is present within it.
	 */
	@Bean
	public ItemWriter<SourceContentDTO> fileItemWriter() throws Exception {
		FlatFileItemWriter<SourceContentDTO> outputFileWriter = new FlatFileItemWriter<>();

		outputFileWriter.setResource(new FileSystemResource(new File(new URI(config.getOUTPUT_FILE()))));

		LineAggregator<SourceContentDTO> lineAggregator = createSourceLineAggregator();
		outputFileWriter.setLineAggregator(lineAggregator);
		outputFileWriter.setShouldDeleteIfEmpty(true);

		return outputFileWriter;
	}

	/*
	 * One of the Spring Batch ItemWriters for the step. This one is responsible for updating the database record
	 * when it has been 'pushed' to the output file.
	 */
	@Bean
	public ItemWriter<SourceContentDTO> updateDBWriter() {
		JdbcBatchItemWriter<SourceContentDTO> toDBWriter = new JdbcBatchItemWriter<SourceContentDTO>();
		toDBWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<SourceContentDTO>());
		toDBWriter.setSql("UPDATE fields SET processed = true WHERE field1 = :field1");
		toDBWriter.setDataSource(dataSource);
		return toDBWriter;
	}
	
	/*
	 * The 'wrapper' writer for the two writers defined above.
	 */
	@Bean
	ItemWriter<SourceContentDTO> outputWriter() throws Exception {
		CompositeItemWriter<SourceContentDTO> writer = new CompositeItemWriter<>();
		List<ItemWriter<? super SourceContentDTO>> allWriters = new ArrayList<>(2);
		allWriters.add(fileItemWriter());
		allWriters.add(updateDBWriter());
		writer.setDelegates(allWriters);
		return writer;
	}
	

	/*
	 * The actual step to be processed by the Job. Is an aggregation of the reader, processor and writer, as
	 * defined above, and these are injected as parameters.
	 * The step is chunked to allow volume commits/rollbacks.
	 */
	@Bean
	public Step datatabaseToFileStep(			
			@Qualifier("dbItemReader") ItemReader<SourceContentDTO> reader,
			@Qualifier("dbContentProcessor") ItemProcessor<SourceContentDTO,SourceContentDTO> processor,
			@Qualifier("outputWriter") ItemWriter<SourceContentDTO> writer,
			DBReadStepCompletionListener listener) throws Exception {

		return stepBuilderFactory.get("datatabaseToFileStep").allowStartIfComplete(true)
				.<SourceContentDTO, SourceContentDTO> chunk(5).reader(reader).processor(processor)
				.writer(writer).listener(listener).build();
	}

	/*
	 * The driving job, which executes the step, by default will be a re-startable job. Therefore, if a 
	 * non-completed instance is found, the previously failed job will try to run to completion.
	 */
	@Bean
	Job databaseToFileJob(@Qualifier("datatabaseToFileStep") Step step,
			DBReadStepCompletionListener listener) throws Exception {
		return jobBuilderFactory.get("databaseToFileJob").incrementer(new RunIdIncrementer())
				.flow(step).end().build();
	}
}
