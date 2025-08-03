package br.com.rinha.pagamentos.health;

import br.com.rinha.pagamentos.model.HealthCheckResponse;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

@Component
public class ProcessorHealthMonitor implements MessageListener {

	private static final Logger log = LoggerFactory.getLogger(ProcessorHealthMonitor.class);

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

		log.info("Inicializando o monitor de saúde. Sincronizando com o estado do Redis...");
		// A sincronização inicial agora é assíncrona
		syncStateFromRedis().subscribe();
	}

	// A anotação @Async não é mais necessária, pois o fluxo é reativo e não bloqueia a thread do scheduler
	@Scheduled(fixedRate = 5000)
	@SchedulerLock(name = "processorHealthCheckLock", lockAtMostFor = "4s", lockAtLeastFor = "4s")
	public void scheduleHealthCheck() {
		log.info("Lock adquirido. Verificando a saúde dos processadores...");
		performHealthCheckAndNotify()
				.doOnError(e -> log.error("Erro na cadeia reativa de verificação de saúde.", e))
				.subscribe(); // .subscribe() inicia a execução da cadeia reativa
	}

	public Mono<Void> performHealthCheckAndNotify() {
		// Combina as duas chamadas de saúde para rodarem em paralelo
		Mono<Boolean> defaultCheck = checkHealthAsync(defaultHealthUrl);
		Mono<Boolean> fallbackCheck = checkHealthAsync(fallbackHealthUrl);

		return Mono.zip(defaultCheck, fallbackCheck)
				.flatMap(results -> {
					boolean isDefaultOk = results.getT1();
					boolean isFallbackOk = results.getT2();

					// Salva os dois status no Redis em paralelo
					Mono<Boolean> setDefault = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_DEFAULT_KEY, isDefaultOk ? "1" : "0");
					Mono<Boolean> setFallback = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_FALLBACK_KEY, isFallbackOk ? "1" : "0");

					return Mono.when(setDefault, setFallback)
							.then(reactiveRedisTemplate.convertAndSend(HEALTH_NOTIFICATION_CHANNEL, "updated"));
				})
				.doOnSuccess(v -> log.info("Saúde verificada e status publicado no Redis."))
				.onErrorResume(e -> { // Em caso de erro em qualquer ponto da cadeia
					log.error("Erro ao verificar ou publicar saúde. Marcando como indisponível.", e);
					Mono<Boolean> setDefault = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_DEFAULT_KEY, "0");
					Mono<Boolean> setFallback = reactiveRedisTemplate.opsForValue().set(HEALTH_STATUS_FALLBACK_KEY, "0");
					// Tenta notificar mesmo em caso de erro
					return Mono.when(setDefault, setFallback)
							.then(reactiveRedisTemplate.convertAndSend(HEALTH_NOTIFICATION_CHANNEL, "updated"));
				})
				.then(); // Converte o resultado final para Mono<Void>
	}

	@Override
	public void onMessage(Message message, byte[] pattern) {
		log.info("Notificação de atualização de saúde recebida. Sincronizando estado local com o Redis.");
		syncStateFromRedis().subscribe();
	}

	private Mono<Void> syncStateFromRedis() {
		return reactiveRedisTemplate.opsForValue().multiGet(List.of(HEALTH_STATUS_DEFAULT_KEY, HEALTH_STATUS_FALLBACK_KEY))
				.doOnSuccess(statuses -> {
					if (statuses != null && statuses.size() == 2) {
						// Garante que não haja NullPointerException se uma chave não existir
						this.isDefaultAvailable = Objects.equals(statuses.get(0), "1");
						this.isFallbackAvailable = Objects.equals(statuses.get(1), "1");
						log.info("Estado local sincronizado: Default [{}], Fallback [{}]",
								isDefaultAvailable ? "Disponível" : "Indisponível",
								isFallbackAvailable ? "Disponível" : "Indisponível");
					} else {
						// Caso inicial onde as chaves podem não existir, assume indisponibilidade (mais seguro)
						this.isDefaultAvailable = false;
						this.isFallbackAvailable = false;
						log.warn("Chaves de saúde não encontradas no Redis. Assumindo indisponibilidade.");
					}
				})
				.doOnError(e -> {
					log.error("Falha ao sincronizar estado de saúde do Redis. Assumindo indisponibilidade.", e);
					this.isDefaultAvailable = false;
					this.isFallbackAvailable = false;
				})
				.then(); // Converte para Mono<Void>
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
