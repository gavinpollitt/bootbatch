package uk.gov.hmrc.regen;

import java.sql.ResultSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("============ JOB FINISHED ============ Verifying the results....\n");

			List<FileContentDTO> results = jdbcTemplate.query("SELECT field1, field2, field3 FROM FIELDS", 
					(ResultSet rs, int row) -> new FileContentDTO(rs.getString(1), rs.getString(2), rs.getString(3)));

			results.forEach((x) -> log.info("Discovered <" + x + "> in the database."));
		}
		
		System.out.println("Spring tables-->");
		List<BatchStepInstance> results = jdbcTemplate.query("SELECT step_execution_id, job_execution_id, status, exit_code FROM BATCH_STEP_EXECUTION", 
				(ResultSet rs, int row) -> new BatchStepInstance(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4)));
		results.forEach((x) -> log.info("Spring Batch <" + x + "> in the database."));
	}
	
	public static class BatchStepInstance {
		private Long jeid;
		private Long jid;
		private String status;
		private String exitCode;
		
		public BatchStepInstance(Long jeid, Long jid, String status, String exitCode) {
			this.jeid = jeid;
			this.jid = jid;
			this.status = status;
			this.exitCode = exitCode;
		}
		
		public Long getJeid() {
			return jeid;
		}
		public void setJeid(Long jeid) {
			this.jeid = jeid;
		}
		public Long getJid() {
			return jid;
		}
		public void setJid(Long jid) {
			this.jid = jid;
		}
		public String getStatus() {
			return status;
		}
		public void setStatus(String status) {
			this.status = status;
		}
		public String getExitCode() {
			return exitCode;
		}
		public void setExitCode(String exitCode) {
			this.exitCode = exitCode;
		}
		
		public String toString() {
			return this.jeid + "::" + this.jid + "::" + this.getStatus() + "::" + this.getExitCode();
		}
		
	}
}



