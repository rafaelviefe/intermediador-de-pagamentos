package br.com.rinha.pagamentos.controller.dto;

import java.math.BigDecimal;

public record PaymentRequest(String correlationId, BigDecimal amount) {
}
