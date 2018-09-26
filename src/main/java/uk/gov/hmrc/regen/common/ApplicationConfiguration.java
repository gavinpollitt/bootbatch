package uk.gov.hmrc.regen.common;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 
 * @author gp
 * Set up the application properties from the appropriate YAML profile
 * Could have used the annotation @ConfigurationProperties; however, this requires
 * setters and could inadvertently result in the properties being corrupted externally.
 *
 */
@Configuration
public class ApplicationConfiguration {
	@Value("${input.inputFile}")
	private String INPUT_FILE;

	@Value("${input.processedFile}")
	private String PROCESSED_FILE;

	@Value("${input.errorDir}")
	private String ERROR_DIR;

	private Path ERROR_PATH;

	@Value("${input.errorFile}")
	private String ERROR_FILE;

	@Value("${input.outputFile}")
	private String OUTPUT_FILE;

	public String getINPUT_FILE() {
		return this.INPUT_FILE;
	}

	public String getPROCESSED_FILE() {
		return this.PROCESSED_FILE;
	}

	public String getERROR_DIR() {
		return this.ERROR_DIR;
	}

	public Path getERROR_PATH() {
		if (this.ERROR_PATH == null) {
			try {
				this.ERROR_PATH = Paths.get(new URI(ERROR_DIR));
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage());
			}
		}

		return this.ERROR_PATH;
	}

	public String getERROR_FILE() {
		return ERROR_FILE;
	}

	public String getOUTPUT_FILE() {
		return OUTPUT_FILE;
	}
}
