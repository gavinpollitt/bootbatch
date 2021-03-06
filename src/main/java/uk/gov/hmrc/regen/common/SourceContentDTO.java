package uk.gov.hmrc.regen.common;

import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class SourceContentDTO {

	@Size(min = 3, max = 30)
	@Pattern(regexp="^[A-Z].* - fieldC1.*")
	private String field1;
	
	@Size(min = 3, max = 30)
	private String field2;
	
	@Size(min = 3, max = 30)
	private String field3;
	
	private Boolean processed = null;

	public SourceContentDTO() {

	}

	public SourceContentDTO(final String field1, final String field2, final String field3, final Boolean processed) {
		this.field1 = field1;
		this.field2 = field2;
		this.field3 = field3;
		this.processed = processed;
	}

	public SourceContentDTO(final String field1, final String field2, final String field3) {
		this(field1,field2,field3,false);
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("Field 1:", this.field1).append(" Field 2:", this.field2)
				.append(" Field 3:", this.field3).toString().concat(this.processed!=null&&this.processed?" and has been processed":" not yet processed");
	}

	public String getField1() {
		return field1;
	}

	public void setField1(String field1) {
		this.field1 = field1;
	}

	public String getField2() {
		return field2;
	}

	public void setField2(String field2) {
		this.field2 = field2;
	}

	public String getField3() {
		return field3;
	}

	public void setField3(String field3) {
		this.field3 = field3;
	}

	public Boolean getProcessed() {
		return processed;
	}

	public void setProcessed(Boolean processed) {
		this.processed = processed;
	}

}
