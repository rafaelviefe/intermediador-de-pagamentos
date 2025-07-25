package br.com.rinha.pagamentos.model;

import java.math.BigDecimal;

public class PaymentReceived {

	private String correlationId;
	private BigDecimal amount;

	public PaymentReceived(BigDecimal amount, String correlationId) {
		this.amount = amount;
		this.correlationId = correlationId;
	}

	public String getCorrelationId() {
		return correlationId;
	}

	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}
}
