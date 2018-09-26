package uk.gov.hmrc.regen.in;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import uk.gov.hmrc.regen.common.SourceContentDTO;

/**
 * 
 * @author gp
 * 
 * Triggers the hibernate validation annotations on the content DTO. Will throw a ValidationException which
 * will be intercepted by the job and terminate the job.
 *
 */
@Component
public class RecordValidationListener implements ItemReadListener<SourceContentDTO> {

	private static final Logger log = LoggerFactory.getLogger(RecordValidationListener.class);

	@Autowired
	private Validator validator;
	
	@Override
	public void beforeRead() {
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.batch.core.ItemReadListener#afterRead(java.lang.Object)
	 * When a line has been read, ensure it is valid. If not, extract the issues and report
	 * the exception
	 * 
	 */
	@Override
	public void afterRead(SourceContentDTO item) {
		log.debug("Validating " + item);
		
		Set<ConstraintViolation<SourceContentDTO>> violations = validator.validate(item);
		
		StringBuffer buff = new StringBuffer();
		if (!violations.isEmpty()) {
			violations.forEach(v -> buff.append(v.getPropertyPath() + ":" + v.getInvalidValue() + "-->" + v.getMessage() + "\n"));
			
			log.error(buff.toString());
			throw new ValidationException(buff.toString());
		}
	}

	@Override
	public void onReadError(Exception ex) {
	}

}
