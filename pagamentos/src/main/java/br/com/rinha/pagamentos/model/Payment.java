package br.com.rinha.pagamentos.model;

import java.math.BigDecimal;
import java.time.Instant;
import br.com.rinha.pagamentos.controller.dto.PaymentRequest;

public class Payment {

	private String correlationId;
	private BigDecimal amount;
	private Instant requestedAt;
	private int retries;

	public Payment(PaymentRequest request) {
		this.correlationId = request.correlationId();
		this.amount = request.amount();
		this.requestedAt = Instant.now();
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

	public Instant getRequestedAt() {
		return requestedAt;
	}

	public void setRequestedAt(Instant requestedAt) {
		this.requestedAt = requestedAt;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}
}
