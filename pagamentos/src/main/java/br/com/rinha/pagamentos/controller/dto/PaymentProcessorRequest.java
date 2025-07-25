package br.com.rinha.pagamentos.controller.dto;

import java.math.BigDecimal;
import java.time.Instant;
import br.com.rinha.pagamentos.model.Payment;

public record PaymentProcessorRequest(
		String correlationId,
		BigDecimal amount,
		Instant requestedAt
) {
	public PaymentProcessorRequest(Payment payment) {
		this(payment.getCorrelationId(), payment.getAmount(), payment.getRequestedAt());
	}
}
