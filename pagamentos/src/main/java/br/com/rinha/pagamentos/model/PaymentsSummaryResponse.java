package br.com.rinha.pagamentos.model;

import java.math.BigDecimal;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PaymentsSummaryResponse {

	public static final PaymentsSummaryResponse EMPTY = new PaymentsSummaryResponse(new Summary(0, BigDecimal.ZERO), new Summary(0, BigDecimal.ZERO));

	@JsonProperty("default")
	private Summary defaultSummary;

	@JsonProperty("fallback")
	private Summary fallbackSummary;

	public PaymentsSummaryResponse() {
	}

	@JsonCreator
	public PaymentsSummaryResponse(
			@JsonProperty("default") Summary defaultSummary,
			@JsonProperty("fallback") Summary fallbackSummary) {
		this.defaultSummary = defaultSummary;
		this.fallbackSummary = fallbackSummary;
	}

	public Summary getDefaultSummary() {
		return defaultSummary;
	}

	public void setDefaultSummary(Summary defaultSummary) {
		this.defaultSummary = defaultSummary;
	}

	public Summary getFallbackSummary() {
		return fallbackSummary;
	}

	public void setFallbackSummary(Summary fallbackSummary) {
		this.fallbackSummary = fallbackSummary;
	}

}
