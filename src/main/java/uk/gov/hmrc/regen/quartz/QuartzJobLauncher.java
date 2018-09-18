package uk.gov.hmrc.regen.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.JobLocator;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class QuartzJobLauncher extends QuartzJobBean {

	private static final Logger log = LoggerFactory.getLogger(QuartzJobLauncher.class);

	private String jobName;
	private JobLauncher jobLauncher;
	private JobLocator jobLocator;

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public JobLauncher getJobLauncher() {
		return jobLauncher;
	}

	public void setJobLauncher(JobLauncher jobLauncher) {
		this.jobLauncher = jobLauncher;
	}

	public JobLocator getJobLocator() {
		return jobLocator;
	}

	public void setJobLocator(JobLocator jobLocator) {
		this.jobLocator = jobLocator;
	}

	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		try {
			Job job = jobLocator.getJob(jobName);
			JobExecution jobExecution = jobLauncher.run(job, new JobParameters());
			
			if (jobExecution.getStatus() == BatchStatus.FAILED) {
				context.setResult(jobExecution);
				log.error("The Spring Batch job terminated early...marked for restart");
			} else {

				log.info("{}_{}_{} was completed successfully", job.getName(), jobExecution.getId(),
						jobExecution.getExitStatus());
			}
		} catch (Exception e) {
			log.error("Encountered job processing exception!" + e);
			throw new JobExecutionException(e);
		}
	}

}
