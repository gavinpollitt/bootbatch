package uk.gov.hmrc.regen.in;

import java.net.MalformedURLException;

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
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.UrlResource;

import uk.gov.hmrc.regen.common.SourceContentDTO;

@EnableBatchProcessing
@Configuration
public class CsvFileToDatabaseConfig {
	private static final Logger log = LoggerFactory.getLogger(CsvFileToDatabaseConfig.class);

	private static final String INPUT_FILE = "file:///home/regen/temp/fileinput/files/process/inputFile.csv";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	public FlatFileItemReader<SourceContentDTO> csvFileReader() throws MalformedURLException {
		log.info("Entering csvFileReader");
		FlatFileItemReader<SourceContentDTO> reader = new FlatFileItemReader<SourceContentDTO>();
		reader.setStrict(false); // Don't fail if file not there

		reader.setResource(new UrlResource(INPUT_FILE));

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

	@Bean
	ItemProcessor<SourceContentDTO, SourceContentDTO> csvFileProcessor() {
		log.info("Entering csvFileProcessor");
		return (fileContentDTO) -> {
			final String field1 = "READ:" + fileContentDTO.getField1();
			final String field2 = "READ:" + fileContentDTO.getField2();
			final String field3 = "READ:" + fileContentDTO.getField3();

			final SourceContentDTO actualFCDTO = new SourceContentDTO(field1, field2, field3);

			log.info("Converting (" + fileContentDTO + ") into (" + actualFCDTO + ")");

			return actualFCDTO;
		};
	}

	@Bean
	public JdbcBatchItemWriter<SourceContentDTO> toDBWriter() {
		log.info("Entering toDBWriter");
		JdbcBatchItemWriter<SourceContentDTO> toDBWriter = new JdbcBatchItemWriter<SourceContentDTO>();
		toDBWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<SourceContentDTO>());
		toDBWriter.setSql("INSERT INTO FIELDS (field1, field2, field3, processed) VALUES (:field1, :field2, :field3, false)");
		toDBWriter.setDataSource(dataSource);
		return toDBWriter;
	}

	// end reader, writer, and processor

	// begin job info
	@Bean
	public Step csvFileToDatabaseStep() throws Exception {
		log.info("Entering csvFileToDatabaseStep");

		return stepBuilderFactory.get("csvFileToDatabaseStep").allowStartIfComplete(true)
				.<SourceContentDTO, SourceContentDTO> chunk(5).reader(csvFileReader()).processor(csvFileProcessor())
				.writer(toDBWriter()).build();
	}

	@Bean
	Job csvFileToDatabaseJob(JobCompletionNotificationListener listener) throws Exception {
		log.info("Entering csvFileToDatabaseJob");
		return jobBuilderFactory.get("csvFileToDatabaseJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(csvFileToDatabaseStep()).end().build();
	}
	// end job info
}
