package uk.gov.hmrc.regen.out;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

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
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import uk.gov.hmrc.regen.common.ApplicationConfiguration;
import uk.gov.hmrc.regen.common.SourceContentDTO;

@EnableBatchProcessing
@Configuration
public class DatabaseToFileConfig {
	private static final Logger log = LoggerFactory.getLogger(DatabaseToFileConfig.class);

	@Autowired
	ApplicationConfiguration config;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	ItemReader<SourceContentDTO> dbItemReader() {
		JdbcCursorItemReader<SourceContentDTO> databaseReader = new JdbcCursorItemReader<>();

		databaseReader.setDataSource(dataSource);
		databaseReader.setSql("SELECT field1, field2, field3 FROM fields WHERE processed is not true");
		databaseReader.setRowMapper(new BeanPropertyRowMapper<>(SourceContentDTO.class));

		return databaseReader;
	}

	@Bean
	ItemProcessor<SourceContentDTO, SourceContentDTO> dbContentProcessor() {
		return (dbContentDTO) -> dbContentDTO;
	}

	private FieldExtractor<SourceContentDTO> createSourceFieldExtractor() {
		BeanWrapperFieldExtractor<SourceContentDTO> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] { "field1", "field2", "field3" });
		return extractor;
	}

	private LineAggregator<SourceContentDTO> createSourceLineAggregator() {
		FormatterLineAggregator<SourceContentDTO> lineAggregator = new FormatterLineAggregator<>();

		FieldExtractor<SourceContentDTO> fieldExtractor = createSourceFieldExtractor();
		lineAggregator.setFieldExtractor(fieldExtractor);
		lineAggregator.setFormat("F1:%-10s*F2:%-10s*F3%-10s");

		return lineAggregator;
	}

	@Bean
	ItemWriter<SourceContentDTO> fileItemWriter() throws Exception {
		FlatFileItemWriter<SourceContentDTO> outputFileWriter = new FlatFileItemWriter<>();

		outputFileWriter.setResource(new FileSystemResource(new File(new URI(config.getOUTPUT_FILE()))));

		LineAggregator<SourceContentDTO> lineAggregator = createSourceLineAggregator();
		outputFileWriter.setLineAggregator(lineAggregator);
		outputFileWriter.setShouldDeleteIfEmpty(true);

		return outputFileWriter;
	}

	@Bean
	public JdbcBatchItemWriter<SourceContentDTO> updateDBWriter() {
		JdbcBatchItemWriter<SourceContentDTO> toDBWriter = new JdbcBatchItemWriter<SourceContentDTO>();
		toDBWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<SourceContentDTO>());
		toDBWriter.setSql("UPDATE fields SET processed = true WHERE field1 = :field1");
		toDBWriter.setDataSource(dataSource);
		return toDBWriter;
	}
	@Bean
	ItemWriter<SourceContentDTO> outputWriter() throws Exception {
		CompositeItemWriter<SourceContentDTO> writer = new CompositeItemWriter<>();
		List<ItemWriter<? super SourceContentDTO>> allWriters = new ArrayList<>(2);
		allWriters.add(fileItemWriter());
		allWriters.add(updateDBWriter());
		writer.setDelegates(allWriters);
		return writer;
	}
	

	// begin job info
	@Bean
	public Step datatabaseToFileStep(DBReadStepCompletionListener listener) throws Exception {

		return stepBuilderFactory.get("datatabaseToFileStep").allowStartIfComplete(true)
				.<SourceContentDTO, SourceContentDTO> chunk(5).reader(dbItemReader()).processor(dbContentProcessor())
				.writer(outputWriter()).listener(listener).build();
	}

	@Bean
	Job databaseToFileJob(DBReadStepCompletionListener listener) throws Exception {
		return jobBuilderFactory.get("databaseToFileJob").incrementer(new RunIdIncrementer())
				.flow(datatabaseToFileStep(listener)).end().build();
	}
	// end job info
}
