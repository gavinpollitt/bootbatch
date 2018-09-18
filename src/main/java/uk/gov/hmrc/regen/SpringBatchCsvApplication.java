package uk.gov.hmrc.regen;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableTransactionManagement
public class SpringBatchCsvApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchCsvApplication.class, args);
	}

}
