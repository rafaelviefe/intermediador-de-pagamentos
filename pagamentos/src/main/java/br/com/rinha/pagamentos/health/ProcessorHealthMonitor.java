package br.com.rinha.pagamentos.health;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import java.util.concurrent.CompletableFuture;
import br.com.rinha.pagamentos.model.HealthCheckResponse;

@Component
public class ProcessorHealthMonitor {

	private final WebClient webClient;

	private final String defaultHealthUrl;
	private final String fallbackHealthUrl;

	private volatile boolean isDefaultAvailable = false;
	private volatile boolean isFallbackAvailable = false;

	public ProcessorHealthMonitor(
			WebClient.Builder webClientBuilder,
			@Value("${processor.default.health.url}") String defaultHealthUrl,
			@Value("${processor.fallback.health.url}") String fallbackHealthUrl) {
		this.webClient = webClientBuilder.build();
		this.defaultHealthUrl = defaultHealthUrl;
		this.fallbackHealthUrl = fallbackHealthUrl;
	}

	@Async("virtualThreadExecutor")
	@Scheduled(fixedRate = 5000)
	public void checkProcessorsHealth() {

		CompletableFuture<Boolean> defaultCheck = checkHealthAsync(defaultHealthUrl);
		CompletableFuture<Boolean> fallbackCheck = checkHealthAsync(fallbackHealthUrl);

		CompletableFuture.allOf(defaultCheck, fallbackCheck).join();

		try {

			this.isDefaultAvailable = defaultCheck.get();
			this.isFallbackAvailable = fallbackCheck.get();

		} catch (Exception e) {

			this.isDefaultAvailable = false;
			this.isFallbackAvailable = false;
		}
	}

	private CompletableFuture<Boolean> checkHealthAsync(String url) {
		return webClient.get()
				.uri(url)
				.retrieve()
				.bodyToMono(HealthCheckResponse.class)
				.map(response -> !response.isFailing())
				.onErrorReturn(false)
				.toFuture();
	}

	public boolean isDefaultProcessorAvailable() {
		return isDefaultAvailable;
	}

	public boolean isFallbackProcessorAvailable() {
		return isFallbackAvailable;
	}
}
