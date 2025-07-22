package br.com.rinha.pagamentos.model;

public class ProcessorHealthStatus {
	private boolean failing;
	private long responseTimeMillis;

	public ProcessorHealthStatus() {
	}

	public ProcessorHealthStatus(boolean failing, long responseTimeMillis) {
		this.failing = failing;
		this.responseTimeMillis = responseTimeMillis;
	}

	public boolean isFailing() {
		return failing;
	}

	public long getResponseTimeMillis() {
		return responseTimeMillis;
	}

	public void setFailing(boolean failing) {
		this.failing = failing;
	}

	public void setResponseTimeMillis(long responseTimeMillis) {
		this.responseTimeMillis = responseTimeMillis;
	}
}
