package br.com.rinha.pagamentos.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Component
public class ProcessorHealthChecker {

	private final RedisTemplate<String, String> redisTemplate;
	private final RestTemplate restTemplate = new RestTemplate();
	private static final String HEALTH_FAILING_PREFIX = "health:failing:";

	@Value("${processor.default.health.url}")
	private String processorDefaultUrl;

	@Value("${processor.fallback.health.url}")
	private String processorFallbackUrl;

	public ProcessorHealthChecker(@Qualifier("healthCheckRedisTemplate") RedisTemplate<String, String> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	@Scheduled(fixedDelay = 5500)
	@SchedulerLock(name = "processorHealthCheck")
	public void checkProcessors() {
		checkProcessorAsync("default", processorDefaultUrl);
		checkProcessorAsync("fallback", processorFallbackUrl);
	}

	@Async("virtualThreadExecutor")
	public void checkProcessorAsync(String processorKey, String url) {
		String healthKey = HEALTH_FAILING_PREFIX + processorKey;
		try {
			ResponseEntity<JsonNode> response = restTemplate.getForEntity(url, JsonNode.class);
			JsonNode body = response.getBody();
			boolean failing = body != null && body.get("failing").asBoolean();

			if (failing) {
				redisTemplate.opsForValue().set(healthKey, "1", Duration.ofSeconds(30));
			} else {
				redisTemplate.delete(healthKey);
			}
		} catch (Exception e) {
			redisTemplate.opsForValue().set(healthKey, "1", Duration.ofSeconds(30));
		}
	}
}
