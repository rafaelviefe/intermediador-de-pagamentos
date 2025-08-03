package br.com.rinha.pagamentos.service;

import br.com.rinha.pagamentos.health.ProcessorHealthMonitor;
import br.com.rinha.pagamentos.model.PaymentsSummaryResponse;
import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.model.PaymentSent;
import br.com.rinha.pagamentos.model.Summary;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Service
public class PaymentService {
	private static final String PAYMENTS_AMOUNT_TS_KEY = "payments:amount:ts";
	private static final String PAYMENTS_COUNT_TS_KEY = "payments:count:ts";
	private static final String PROCESSING_QUEUE_KEY = "payments:processing-queue";
	private static final BigDecimal ONE_HUNDRED = new BigDecimal("100");
	private static final RedisScript<Long> PERSIST_PAYMENT_SCRIPT =
			new DefaultRedisScript<>(
					"redis.call('TS.MADD', KEYS[1], ARGV[1], ARGV[2], KEYS[2], ARGV[1], 1); return 1",
					Long.class
			);
	private static final RedisScript<List> GET_SUMMARY_SCRIPT =
			new DefaultRedisScript<>(
					"""
					local timeBucket = 9999999999999
					local summary = {0, 0, 0, 0}
					local default_sum_res = redis.call('TS.RANGE', KEYS[1], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #default_sum_res > 0 then summary[2] = default_sum_res[1][2] end
					local default_count_res = redis.call('TS.RANGE', KEYS[2], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #default_count_res > 0 then summary[1] = default_count_res[1][2] end
					local fallback_sum_res = redis.call('TS.RANGE', KEYS[3], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #fallback_sum_res > 0 then summary[4] = fallback_sum_res[1][2] end
					local fallback_count_res = redis.call('TS.RANGE', KEYS[4], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #fallback_count_res > 0 then summary[3] = fallback_count_res[1][2] end
					return summary
					""",
					List.class
			);
	private final RedisTemplate<String, QueuedPayment> queuedRedisTemplate;
	private final RedisTemplate<String, String> persistedRedisTemplate;
	private final ReactiveStringRedisTemplate reactivePersistedRedisTemplate;
	private final WebClient webClient;
	private final ProcessorHealthMonitor healthMonitor;

	@Value("${processor.default.payments.url}")
	private String processorDefaultUrl;
	@Value("${processor.fallback.payments.url}")
	private String processorFallbackUrl;

	public PaymentService(
			@Qualifier("queuedRedisTemplate") RedisTemplate<String, QueuedPayment> queuedRedisTemplate,
			@Qualifier("persistedRedisTemplate") RedisTemplate<String, String> persistedRedisTemplate,
			@Qualifier("reactivePersistedRedisTemplate") ReactiveStringRedisTemplate reactivePersistedRedisTemplate,
			WebClient.Builder webClientBuilder,
			ProcessorHealthMonitor healthMonitor) {
		this.queuedRedisTemplate = queuedRedisTemplate;
		this.persistedRedisTemplate = persistedRedisTemplate;
		this.reactivePersistedRedisTemplate = reactivePersistedRedisTemplate;
		this.webClient = webClientBuilder.build();
		this.healthMonitor = healthMonitor;
	}

	@Async("virtualThreadExecutor")
	public void handlePayment(QueuedPayment payment) {
		processPayment(payment);
	}

	public void processPayment(QueuedPayment payment) {
		final boolean isDefaultUp = healthMonitor.isDefaultProcessorAvailable();
		final boolean isFallbackUp = healthMonitor.isFallbackProcessorAvailable();
		final PaymentSent paymentSent = new PaymentSent(payment);

		if (isDefaultUp && isFallbackUp) {
			trySendAndPersist("default", processorDefaultUrl, paymentSent)
					.filter(Boolean::booleanValue)
					.switchIfEmpty(trySendAndPersist("fallback", processorFallbackUrl, paymentSent))
					.filter(Boolean::booleanValue)
					.switchIfEmpty(requeue(payment))
					.subscribe();
		} else if (isDefaultUp) {
			trySendAndPersist("default", processorDefaultUrl, paymentSent)
					.filter(Boolean::booleanValue)
					.switchIfEmpty(requeue(payment))
					.subscribe();
		} else if (isFallbackUp) {
			trySendAndPersist("fallback", processorFallbackUrl, paymentSent)
					.filter(Boolean::booleanValue)
					.switchIfEmpty(requeue(payment))
					.subscribe();
		} else {
			queuePayment(payment);
		}
	}

	public void queuePayment(QueuedPayment payment) {
		queuedRedisTemplate.opsForList().leftPush(PROCESSING_QUEUE_KEY, payment);
	}

	private Mono<Boolean> requeue(QueuedPayment payment) {
		return Mono.fromRunnable(() -> queuePayment(payment))
				.then(Mono.just(false));
	}

	private Mono<Boolean> trySendAndPersist(String processorKey, String url, PaymentSent paymentSent) {
		return webClient.post()
				.uri(url)
				.bodyValue(paymentSent)
				.exchangeToMono(response -> {
					if (response.statusCode().is2xxSuccessful()) {
						return persistSuccessfulPaymentReactive(paymentSent, processorKey)
								.thenReturn(true);
					}
					if (response.statusCode().isError()) {
						return Mono.empty();
					}
					return Mono.just(false);
				})
				.onErrorResume(e -> Mono.empty());
	}

	private Mono<Long> persistSuccessfulPaymentReactive(PaymentSent paymentSent, String processorKey) {
		String amountKey = PAYMENTS_AMOUNT_TS_KEY + ":" + processorKey;
		String countKey = PAYMENTS_COUNT_TS_KEY + ":" + processorKey;

		String timestamp = String.valueOf(paymentSent.getRequestedAt().toEpochMilli());
		String amount = String.valueOf(paymentSent.getAmount().multiply(ONE_HUNDRED).longValue());

		return reactivePersistedRedisTemplate.execute(
				PERSIST_PAYMENT_SCRIPT,
				List.of(amountKey, countKey),
				List.of(timestamp, amount)
		).next();
	}

	public PaymentsSummaryResponse getPaymentsSummary(String from, String to) {

		List<Object> results = Optional.ofNullable(
				persistedRedisTemplate.execute(
						GET_SUMMARY_SCRIPT,
						List.of(PAYMENTS_AMOUNT_TS_KEY + ":default", PAYMENTS_COUNT_TS_KEY + ":default", PAYMENTS_AMOUNT_TS_KEY + ":fallback", PAYMENTS_COUNT_TS_KEY + ":fallback"),
						(from != null) ? String.valueOf(Instant.parse(from).toEpochMilli()) : "-",
						(to != null) ? String.valueOf(Instant.parse(to).toEpochMilli()) : "+"
				)
		).orElse(Collections.emptyList());

		long defaultCount = 0;
		long defaultAmountCents = 0;
		long fallbackCount = 0;
		long fallbackAmountCents = 0;
		if (results.size() == 4) {
			defaultCount = Long.parseLong(String.valueOf(results.get(0)));
			defaultAmountCents = Long.parseLong(String.valueOf(results.get(1)));
			fallbackCount = Long.parseLong(String.valueOf(results.get(2)));
			fallbackAmountCents = Long.parseLong(String.valueOf(results.get(3)));
		}

		Summary defaultSummary = new Summary(
				defaultCount,
				BigDecimal.valueOf(defaultAmountCents).divide(ONE_HUNDRED, 2, RoundingMode.UNNECESSARY)
		);
		Summary fallbackSummary = new Summary(
				fallbackCount,
				BigDecimal.valueOf(fallbackAmountCents).divide(ONE_HUNDRED, 2, RoundingMode.UNNECESSARY)
		);
		return new PaymentsSummaryResponse(defaultSummary, fallbackSummary);
	}
}
