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

@Component
public class RecordValidationListener implements ItemReadListener<SourceContentDTO> {

	private static final Logger log = LoggerFactory.getLogger(RecordValidationListener.class);

	@Autowired
	private Validator validator;
	
	@Override
	public void beforeRead() {
	}

	@Override
	public void afterRead(SourceContentDTO item) {
		log.info("Validating " + item);
		
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
