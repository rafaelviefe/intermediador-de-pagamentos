package br.com.rinha.pagamentos.model;

import java.math.BigDecimal;
import java.util.UUID;

public class QueuedPayment {

	private UUID correlationId;
	private BigDecimal amount;

	public QueuedPayment() {
	}

	public QueuedPayment(PaymentReceived request) {
		this.correlationId = request.getCorrelationId();
		this.amount = request.getAmount();
	}

	public UUID getCorrelationId() {
		return correlationId;
	}

	public void setCorrelationId(UUID correlationId) {
		this.correlationId = correlationId;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}
}
