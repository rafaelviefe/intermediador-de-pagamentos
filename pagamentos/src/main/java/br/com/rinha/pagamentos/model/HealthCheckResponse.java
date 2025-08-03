package br.com.rinha.pagamentos.model;

public class HealthCheckResponse {

	private boolean failing;

	public HealthCheckResponse() {
	}

	public HealthCheckResponse(boolean failing) {
		this.failing = failing;
	}

	public boolean isFailing() {
		return failing;
	}

	public void setFailing(boolean failing) {
		this.failing = failing;
	}

}
