package uk.gov.hmrc.regen.out;

import java.io.File;
import java.net.URI;

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
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import uk.gov.hmrc.regen.common.SourceContentDTO;
import uk.gov.hmrc.regen.in.JobCompletionNotificationListener;

@EnableBatchProcessing
@Configuration
public class DatabaseToFileConfig {
	private static final Logger log = LoggerFactory.getLogger(DatabaseToFileConfig.class);

	private static final String OUTPUT_FILE = "file:///home/regen/temp/fileinput/files/output/outputFile";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	ItemReader<SourceContentDTO> dbItemReader() {
		log.info("Entering databaseItemReader");
		JdbcCursorItemReader<SourceContentDTO> databaseReader = new JdbcCursorItemReader<>();

		databaseReader.setDataSource(dataSource);
		databaseReader.setSql("SELECT field1, field2, field3 FROM fields");
		databaseReader.setRowMapper(new BeanPropertyRowMapper<>(SourceContentDTO.class));

		return databaseReader;
	}

	@Bean
	ItemProcessor<SourceContentDTO, SourceContentDTO> dbContentProcessor() {
		log.info("Entering dbContentProcessor");
		return (dbContentDTO) -> {
			final String field1 = "READ:" + dbContentDTO.getField1();
			final String field2 = "READ:" + dbContentDTO.getField2();
			final String field3 = "READ:" + dbContentDTO.getField3();

			final SourceContentDTO actualFCDTO = new SourceContentDTO(field1, field2, field3);

			log.info("Converting (" + dbContentDTO + ") into (" + actualFCDTO + ")");

			return actualFCDTO;
		};
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

		outputFileWriter.setResource(new FileSystemResource(new File(new URI(OUTPUT_FILE))));

		LineAggregator<SourceContentDTO> lineAggregator = createSourceLineAggregator();
		outputFileWriter.setLineAggregator(lineAggregator);

		return outputFileWriter;
	}

	// begin job info
	@Bean
	public Step datatabaseToFileStep() throws Exception {
		log.info("Entering csvFileToDatabaseStep");

		return stepBuilderFactory.get("datatabaseToFileStep").allowStartIfComplete(true)
				.<SourceContentDTO, SourceContentDTO> chunk(5).reader(dbItemReader()).processor(dbContentProcessor())
				.writer(fileItemWriter()).build();
	}

	@Bean
	Job databaseToFileJob(JobCompletionNotificationListener listener) throws Exception {
		log.info("Entering csvFileToDatabaseJob");
		return jobBuilderFactory.get("databaseToFileJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(datatabaseToFileStep()).end().build();
	}
	// end job info
}
