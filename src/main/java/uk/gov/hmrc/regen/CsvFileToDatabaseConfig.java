package uk.gov.hmrc.regen;

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
import org.springframework.core.io.ClassPathResource;

@EnableBatchProcessing
@Configuration
public class CsvFileToDatabaseConfig {
    private static final Logger log = LoggerFactory.getLogger(CsvFileToDatabaseConfig.class);

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	public FlatFileItemReader<FileContentDTO> csvFileReader() {
		log.info("Entering csvFileReader");
		FlatFileItemReader<FileContentDTO> reader = new FlatFileItemReader<FileContentDTO>();
		reader.setResource(new ClassPathResource("inputFile.csv"));
		reader.setLineMapper(new DefaultLineMapper<FileContentDTO>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "field1", "field2", "field3" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<FileContentDTO>() {
					{
						setTargetType(FileContentDTO.class);
					}
				});
			}
		});
		return reader;
	}

	@Bean
	ItemProcessor<FileContentDTO, FileContentDTO> csvFileProcessor() {
		log.info("Entering csvFileProcessor");
		return (fileContentDTO) -> {	
			final String field1 = fileContentDTO.getField1();
			final String field2 = fileContentDTO.getField2();
			final String field3 = fileContentDTO.getField3();

			final FileContentDTO actualFCDTO = new FileContentDTO(field1, field2, field3);

			log.info("Converting (" + fileContentDTO + ") into (" + actualFCDTO + ")");

			return actualFCDTO;
		};	
	}
	
	@Bean
	public JdbcBatchItemWriter<FileContentDTO> toDBWriter() {
		log.info("Entering toDBWriter");
		JdbcBatchItemWriter<FileContentDTO> toDBWriter = new JdbcBatchItemWriter<FileContentDTO>();
		toDBWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<FileContentDTO>());
		toDBWriter.setSql("INSERT INTO FIELDS (field1, field2, field3) VALUES (:field1, :field2, :field3)");
		toDBWriter.setDataSource(dataSource);
		return toDBWriter;
	}

	// end reader, writer, and processor

	// begin job info
	@Bean
	public Step csvFileToDatabaseStep() {
		log.info("Entering csvFileToDatabaseStep");
		return stepBuilderFactory.get("csvFileToDatabaseStep").<FileContentDTO, FileContentDTO> chunk(5)
				.reader(csvFileReader()).processor(csvFileProcessor()).writer(toDBWriter()).build();
	}

	@Bean
	Job csvFileToDatabaseJob(JobCompletionNotificationListener listener) {
		log.info("Entering csvFileToDatabaseJob");
		return jobBuilderFactory.get("csvFileToDatabaseJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(csvFileToDatabaseStep()).end().build();
	}
	// end job info
}
