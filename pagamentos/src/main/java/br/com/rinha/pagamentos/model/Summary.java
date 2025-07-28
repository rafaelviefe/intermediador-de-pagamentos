package br.com.rinha.pagamentos.model;

import java.math.BigDecimal;

public class Summary {

	private long totalRequests;
	private BigDecimal totalAmount;

	public Summary() {
	}

	public Summary(long totalRequests, BigDecimal totalAmount) {
		this.totalAmount = totalAmount;
		this.totalRequests = totalRequests;
	}

	public BigDecimal getTotalAmount() {
		return totalAmount;
	}

	public void setTotalAmount(BigDecimal totalAmount) {
		this.totalAmount = totalAmount;
	}

	public long getTotalRequests() {
		return totalRequests;
	}

	public void setTotalRequests(long totalRequests) {
		this.totalRequests = totalRequests;
	}
}
