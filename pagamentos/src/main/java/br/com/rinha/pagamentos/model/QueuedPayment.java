package br.com.rinha.pagamentos.model;

import java.math.BigDecimal;

public class QueuedPayment {

	private String correlationId;
	private BigDecimal amount;
	private int retries;

	public QueuedPayment(PaymentReceived request) {
		this.correlationId = request.getCorrelationId();
		this.amount = request.getAmount();
		this.retries = 0;
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

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}
}
