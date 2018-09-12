package uk.gov.hmrc.regen.quartz;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.configuration.JobLocator;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

@Configuration
public class QuartzConfiguration {
	
	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobLocator jobLocator;

	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
		JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
		jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
		return jobRegistryBeanPostProcessor;
	}
	
	@Bean
	public JobDetailFactoryBean jobDetailFactoryBean() {
		JobDetailFactoryBean factory = new JobDetailFactoryBean();
		factory.setJobClass(QuartzJobLauncher.class);
		Map<String,Object> map = new HashMap<>();
		map.put("jobName", "csvFileToDatabaseJob");
		map.put("jobLauncher", jobLauncher);
		map.put("jobLocator", jobLocator);
		factory.setJobDataAsMap(map);
		factory.setGroup("csv_group");
		factory.setName("csv_job");
		return factory;
	}

	// Job is scheduled after every 2 minute
	@Bean
	public CronTriggerFactoryBean cronTriggerFactoryBean() {
		CronTriggerFactoryBean stFactory = new CronTriggerFactoryBean();
		stFactory.setJobDetail(jobDetailFactoryBean().getObject());
		stFactory.setStartDelay(3000);
		stFactory.setName("cron_trigger");
		stFactory.setGroup("cron_group");
		stFactory.setCronExpression("0 0/1 * 1/1 * ? *");
		return stFactory;
	}

	@Bean
	public SchedulerFactoryBean schedulerFactoryBean() {
		SchedulerFactoryBean scheduler = new SchedulerFactoryBean();
		scheduler.setTriggers(cronTriggerFactoryBean().getObject());
		return scheduler;
	}

}

