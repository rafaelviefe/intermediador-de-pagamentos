package br.com.rinha.pagamentos.scheduler;

import br.com.rinha.pagamentos.model.ProcessorHealthStatus;
import com.fasterxml.jackson.databind.JsonNode;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.CompletableFuture;

@Component
public class ProcessorHealthChecker {

	private final RedisTemplate<String, ProcessorHealthStatus> redisTemplate;
	private final RestTemplate restTemplate = new RestTemplate();

	@Value("${processor.default.health.url}")
	private String processorDefaultUrl;

	@Value("${processor.fallback.health.url}")
	private String processorFallbackUrl;

	public ProcessorHealthChecker(RedisTemplate<String, ProcessorHealthStatus> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	@Scheduled(fixedDelay = 5100)
	@SchedulerLock(name = "processorHealthCheck")
	public void checkProcessors() {
		checkProcessorAsync("default", processorDefaultUrl);
		checkProcessorAsync("fallback", processorFallbackUrl);
	}

	@Async("virtualThreadExecutor")
	public CompletableFuture<Void> checkProcessorAsync(String processorKey, String url) {
		try {
			ResponseEntity<JsonNode> response = restTemplate.getForEntity(url, JsonNode.class);
			JsonNode body = response.getBody();
			boolean failing = body.get("failing").asBoolean();
			long responseTime = body.get("minResponseTime").asLong();
			redisTemplate.opsForValue().set("health:" + processorKey, new ProcessorHealthStatus(failing, responseTime));
		} catch (Exception e) {
			redisTemplate.opsForValue().set("health:" + processorKey, new ProcessorHealthStatus(true, 9999));
		}
		return CompletableFuture.completedFuture(null);
	}
}
