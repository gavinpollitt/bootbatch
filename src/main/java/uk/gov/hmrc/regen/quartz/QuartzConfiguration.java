package uk.gov.hmrc.regen.quartz;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.listeners.JobListenerSupport;
import org.quartz.listeners.TriggerListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.JobLocator;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import uk.gov.hmrc.regen.common.ApplicationConfiguration;

@Configuration
public class QuartzConfiguration {

	private static final Logger log = LoggerFactory.getLogger(QuartzConfiguration.class);
	
	@Autowired
	ApplicationConfiguration config;
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private JobLocator jobLocator;

	private static boolean isDirEmpty(final Path directory) throws IOException {
	    try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
	        return !dirStream.iterator().hasNext();
	    }
	}
	
	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
		JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
		jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
		return jobRegistryBeanPostProcessor;
	}

	@Bean
	public JobDetailFactoryBean csvJobDetailFactoryBean() {
		JobDetailFactoryBean factory = new JobDetailFactoryBean();
		factory.setJobClass(QuartzJobLauncher.class);
		Map<String, Object> map = new HashMap<>();
		map.put("jobName", "csvFileToDatabaseJob");
		map.put("jobLauncher", jobLauncher);
		map.put("jobLocator", jobLocator);
		factory.setJobDataAsMap(map);
		factory.setGroup("csv_group");
		factory.setName("csv_job");
		return factory;
	}

	@Bean
	public CronTriggerFactoryBean csvCronTriggerFactoryBean(@Qualifier("csvJobDetailFactoryBean") JobDetailFactoryBean factory) {
		CronTriggerFactoryBean stFactory = new CronTriggerFactoryBean();
		stFactory.setJobDetail(factory.getObject());
		stFactory.setStartDelay(3000);
		stFactory.setName("csv_cron_trigger");
		stFactory.setGroup("csv_group");
		stFactory.setCronExpression("0 0/1 * 1/1 * ? *");
		return stFactory;
	}

	@Bean
	public JobDetailFactoryBean dbJobDetailFactoryBean() {
		JobDetailFactoryBean factory = new JobDetailFactoryBean();
		factory.setJobClass(QuartzJobLauncher.class);
		Map<String, Object> map = new HashMap<>();
		map.put("jobName", "databaseToFileJob");
		map.put("jobLauncher", jobLauncher);
		map.put("jobLocator", jobLocator);
		factory.setJobDataAsMap(map);
		factory.setGroup("csv_group");
		factory.setName("db_job");
		return factory;
	}

	@Bean
	public CronTriggerFactoryBean dbCronTriggerFactoryBean(@Qualifier("dbJobDetailFactoryBean") JobDetailFactoryBean factory) {
		CronTriggerFactoryBean stFactory = new CronTriggerFactoryBean();
		stFactory.setJobDetail(factory.getObject());
		stFactory.setStartDelay(3000);
		stFactory.setName("db_cron_trigger");
		stFactory.setGroup("scv_group");
		stFactory.setCronExpression("30 0/3 * 1/1 * ? *");
		return stFactory;
	}
	
	@Bean
	public SchedulerFactoryBean schedulerFactoryBean(
							@Qualifier("csvCronTriggerFactoryBean") CronTriggerFactoryBean csvTrigger,
							@Qualifier("dbCronTriggerFactoryBean") CronTriggerFactoryBean dbTrigger) throws SchedulerException {
		log.info("Creating the scheduler");
		SchedulerFactoryBean scheduler = new SchedulerFactoryBean();
		scheduler.setTriggers(csvTrigger.getObject(),
							  dbTrigger.getObject());

		scheduler.setGlobalTriggerListeners(new TriggerListenerSupport() {

			@Override
			public String getName() {
				return "VetoNoFileListener";
			}

			@Override
			public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {

				boolean veto = false;

				if (context.getJobDetail().getKey().getName().equals("csv_job")) {
					try {
						veto = context.getScheduler().getCurrentlyExecutingJobs().size() > 0 ||
								!isDirEmpty(config.getERROR_PATH());

						if (veto) {
							this.getLog().info("Veto due to process already executing or awaiting restart");
						} else {
							veto = !(new File(new URI(config.getINPUT_FILE())).exists());
							this.getLog().info("File existence check...Veto trigger " + veto);
						}
					} catch (Exception e) {
						this.getLog().info("File " + config.getINPUT_FILE() + " not located...sleeping");
						this.getLog().error("Error:" + e);
						veto = true;
					}
				}
				return veto;
			}

		});

		scheduler.setGlobalJobListeners(new JobListenerSupport() {

			@Override
			public void jobToBeExecuted(JobExecutionContext context) {
				if (context.getJobDetail().getKey().getName().equals("csv_job")) {
					this.getLog().info("Performing job set-up for csv processing");

					try {
						Path dest = Files.move(Paths.get(new URI(config.getINPUT_FILE())), Paths.get(new URI(config.getPROCESSED_FILE())));

						if (dest == null) {
							throw new Exception("Unable to move file:" + config.getINPUT_FILE() + " to " + config.getPROCESSED_FILE());
						}
					} catch (Exception e) {
						this.getLog().error(e.getMessage());
					}
				}
			}

			@Override
			public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
				log.info("Job: " + context.getJobDetail().getKey().getName() + ", Exception: " + jobException + ", Status: " + context.getResult());
				JobExecution jEx = (JobExecution)context.getResult();
				boolean repairRequired = (jEx != null) && (jEx.getExitStatus().equals(ExitStatus.FAILED));
				if (context.getJobDetail().getKey().getName().equals("csv_job") && jobException == null && !repairRequired) {
					this.getLog().info("Performing job wrap-up for csv processing");

					try {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
						String NO = config.getPROCESSED_FILE() + sdf.format(new Date());
						Path dest = Files.move(Paths.get(new URI(config.getPROCESSED_FILE())), Paths.get(new URI(NO)));

						if (dest == null) {
							throw new Exception("Unable to move file:" + config.getPROCESSED_FILE() + " to " + NO);
						}
					} catch (Exception e) {
						this.getLog().error(e.getMessage());
					}
				}
				else if (context.getJobDetail().getKey().getName().equals("csv_job") && jobException == null && repairRequired) {
					this.getLog().info("Performing error tidy up csv processing");

					try {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
						String NO = config.getERROR_FILE() + sdf.format(new Date());
						Path dest = Files.move(Paths.get(new URI(config.getPROCESSED_FILE())), Paths.get(new URI(NO)));

						if (dest == null) {
							throw new Exception("Unable to move file:" + config.getPROCESSED_FILE() + " to " + NO);
						}
					} catch (Exception e) {
						this.getLog().error(e.getMessage());
					}
				}
				else if (context.getJobDetail().getKey().getName().equals("db_job") && jobException == null) {
					this.getLog().info("Performing job wrap-up for database processing");

					try {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
						String NO = config.getOUTPUT_FILE() + sdf.format(new Date());
						Path dest = Files.move(Paths.get(new URI(config.getOUTPUT_FILE())), Paths.get(new URI(NO)));

						if (dest == null) {
							throw new Exception("Unable to move file:" + config.getOUTPUT_FILE() + " to " + NO);
						}
					} catch (NoSuchFileException fnf) {
						this.getLog().debug("No output file found to timestamp");
					} catch (Exception e) {
						this.getLog().error(e.getClass() + "-->" + e.getMessage());
					}

				}
			}

			@Override
			public String getName() {
				// TODO Auto-generated method stub
				return "BeforeTheJob";
			}
		});

		return scheduler;
	}

}
