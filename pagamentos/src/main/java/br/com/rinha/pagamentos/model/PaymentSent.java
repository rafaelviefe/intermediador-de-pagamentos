package br.com.rinha.pagamentos.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class PaymentSent {

	private UUID correlationId;
	private BigDecimal amount;
	private Instant requestedAt;

	public PaymentSent(QueuedPayment payment) {
		this.amount = payment.getAmount();
		this.correlationId = payment.getCorrelationId();
		this.requestedAt = Instant.now();
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

	public Instant getRequestedAt() {
		return requestedAt;
	}

	public void setRequestedAt(Instant requestedAt) {
		this.requestedAt = requestedAt;
	}
}
