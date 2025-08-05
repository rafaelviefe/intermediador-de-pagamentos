package br.com.rinha.pagamentos.health;

import br.com.rinha.pagamentos.model.HealthCheckResponse;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class ProcessorHealthMonitor implements MessageListener {

	private static final String HEALTH_STATUS_DEFAULT_KEY = "health:status:default";
	private static final String HEALTH_STATUS_FALLBACK_KEY = "health:status:fallback";
	private static final String HEALTH_NOTIFICATION_CHANNEL = "health:notifications";
	private static final ChannelTopic NOTIFICATION_TOPIC = new ChannelTopic(HEALTH_NOTIFICATION_CHANNEL);

	private final WebClient webClient;
	private final ReactiveStringRedisTemplate reactiveRedisTemplate;
	private final String defaultHealthUrl;
	private final String fallbackHealthUrl;

	private volatile boolean isDefaultAvailable = false;
	private volatile boolean isFallbackAvailable = false;

	public ProcessorHealthMonitor(
			WebClient.Builder webClientBuilder,
			@Qualifier("reactivePersistedRedisTemplate") ReactiveStringRedisTemplate reactiveRedisTemplate,
			@Value("${processor.default.health.url}") String defaultHealthUrl,
			@Value("${processor.fallback.health.url}") String fallbackHealthUrl) {

		this.webClient = webClientBuilder.build();
		this.reactiveRedisTemplate = reactiveRedisTemplate;
		this.defaultHealthUrl = defaultHealthUrl;
		this.fallbackHealthUrl = fallbackHealthUrl;

		syncStateFromRedis().subscribe();
	}

	@Async("virtualThreadExecutor")
	@Scheduled(fixedRate = 5150)
	@SchedulerLock(name = "processorHealthCheckLock", lockAtMostFor = "4s", lockAtLeastFor = "4s")
	public void scheduleHealthCheck() {
		performHealthCheckAndNotify().subscribe();
	}

	public Mono<Void> performHealthCheckAndNotify() {
		Mono<Boolean> defaultCheck = checkHealthAsync(defaultHealthUrl);
		Mono<Boolean> fallbackCheck = checkHealthAsync(fallbackHealthUrl);

		return Mono.zip(defaultCheck, fallbackCheck)
				.flatMap(results -> {
					boolean isDefaultOk = results.getT1();
					boolean isFallbackOk = results.getT2();

					Mono<Boolean> setDefault = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_DEFAULT_KEY, isDefaultOk ? "1" : "0");
					Mono<Boolean> setFallback = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_FALLBACK_KEY, isFallbackOk ? "1" : "0");

					return Mono.when(setDefault, setFallback)
							.then(reactiveRedisTemplate.convertAndSend(HEALTH_NOTIFICATION_CHANNEL, "updated"));
				})
				.onErrorResume(e -> {
					Mono<Boolean> setDefault = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_DEFAULT_KEY, "0");
					Mono<Boolean> setFallback = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_FALLBACK_KEY, "0");
					return Mono.when(setDefault, setFallback)
							.then(reactiveRedisTemplate.convertAndSend(HEALTH_NOTIFICATION_CHANNEL, "updated"));
				})
				.then();
	}

	@Override
	public void onMessage(Message message, byte[] pattern) {
		syncStateFromRedis().subscribe();
	}

	private Mono<Void> syncStateFromRedis() {
		return reactiveRedisTemplate.opsForValue().multiGet(List.of(HEALTH_STATUS_DEFAULT_KEY, HEALTH_STATUS_FALLBACK_KEY))
				.doOnSuccess(statuses -> {
					if (statuses != null && statuses.size() == 2) {
						this.isDefaultAvailable = Objects.equals(statuses.get(0), "1");
						this.isFallbackAvailable = Objects.equals(statuses.get(1), "1");
					} else {
						this.isDefaultAvailable = false;
						this.isFallbackAvailable = false;
					}
				})
				.doOnError(e -> {
					this.isDefaultAvailable = false;
					this.isFallbackAvailable = false;
				})
				.then();
	}

	private Mono<Boolean> checkHealthAsync(String url) {
		return webClient.get()
				.uri(url)
				.retrieve()
				.bodyToMono(HealthCheckResponse.class)
				.map(response -> !response.isFailing())
				.onErrorReturn(false);
	}

	public ChannelTopic getTopic() {
		return NOTIFICATION_TOPIC;
	}

	public boolean isDefaultProcessorAvailable() {
		return isDefaultAvailable;
	}

	public boolean isFallbackProcessorAvailable() {
		return isFallbackAvailable;
	}
}
