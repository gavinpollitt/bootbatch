package uk.gov.hmrc.regen.out;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.stereotype.Component;

/**
 * 
 * @author gp
 *
 * Provide the appropriate information in the logs when there will be no output file produced.
 */
@Component
public class DBReadStepCompletionListener extends StepExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(DBReadStepCompletionListener.class);

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (stepExecution.getStatus() == BatchStatus.COMPLETED && stepExecution.getReadCount() == 0) {
			log.info("No records to process on the database - no output file will be generated");
		}
		return stepExecution.getExitStatus();
	}
}
