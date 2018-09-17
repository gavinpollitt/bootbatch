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
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.UrlResource;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import uk.gov.hmrc.regen.common.SourceContentDTO;

@EnableBatchProcessing
@Configuration
public class CsvFileToDatabaseConfig {
	private static final Logger log = LoggerFactory.getLogger(CsvFileToDatabaseConfig.class);

	private static final String INPUT_FILE = "file:///home/regen/temp/fileinput/files/process/inputFile.csv";

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;
	
	@Autowired 
	private RecordValidationListener validationListener;

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
    public Validator validatorFactory () {
        return new LocalValidatorFactoryBean();
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
	ItemProcessor<SourceContentDTO, SourceContentDTO> inputProcessor() {
		CompositeItemProcessor<SourceContentDTO, SourceContentDTO> processor = new CompositeItemProcessor<>();
		List<ItemProcessor<? super SourceContentDTO, ? super SourceContentDTO>> allProcessors = new ArrayList<>(2);
		allProcessors.add(csvFileProcessor());
		processor.setDelegates(allProcessors);
		return processor;
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


	@Bean
	public Step csvFileToDatabaseStep() throws Exception {
		log.info("Entering csvFileToDatabaseStep");

		return stepBuilderFactory.get("csvFileToDatabaseStep").allowStartIfComplete(true).
				<SourceContentDTO, SourceContentDTO> chunk(5).
				faultTolerant().noSkip(ValidationException.class).
				reader(csvFileReader()).
				processor(csvFileProcessor()).
				writer(toDBWriter()).
				listener(validationListener).
				build();
	}

	@Bean
	Job csvFileToDatabaseJob(FileReadCompletionListener listener) throws Exception {
		log.info("Entering csvFileToDatabaseJob");
		return jobBuilderFactory.get("csvFileToDatabaseJob").incrementer(new RunIdIncrementer()).listener(listener)
				.flow(csvFileToDatabaseStep()).end().build();
	}
	// end job info
}
