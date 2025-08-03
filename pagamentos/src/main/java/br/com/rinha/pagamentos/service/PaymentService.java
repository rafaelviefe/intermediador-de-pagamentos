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
import java.time.Instant;
import java.util.List;

@Service
public class PaymentService {
	private static final String PAYMENTS_AMOUNT_TS_KEY = "payments:amount:ts";
	private static final String PAYMENTS_COUNT_TS_KEY = "payments:count:ts";
	private static final String PROCESSING_QUEUE_KEY = "payments:processing-queue";
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
					local default_amount_res = redis.call('TS.RANGE', KEYS[1], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #default_amount_res > 0 then summary[2] = default_amount_res[1][2] end
					local default_count_res = redis.call('TS.RANGE', KEYS[2], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #default_count_res > 0 then summary[1] = default_count_res[1][2] end
					local fallback_amount_res = redis.call('TS.RANGE', KEYS[3], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #fallback_amount_res > 0 then summary[4] = fallback_amount_res[1][2] end
					local fallback_count_res = redis.call('TS.RANGE', KEYS[4], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					if #fallback_count_res > 0 then summary[3] = fallback_count_res[1][2] end
					return summary
					""",
					List.class
			);
	private final RedisTemplate<String, QueuedPayment> queuedRedisTemplate;
	private final ReactiveStringRedisTemplate reactivePersistedRedisTemplate;
	private final WebClient webClient;
	private final ProcessorHealthMonitor healthMonitor;

	@Value("${processor.default.payments.url}")
	private String processorDefaultUrl;
	@Value("${processor.fallback.payments.url}")
	private String processorFallbackUrl;

	public PaymentService(
			@Qualifier("queuedRedisTemplate") RedisTemplate<String, QueuedPayment> queuedRedisTemplate,
			@Qualifier("reactivePersistedRedisTemplate") ReactiveStringRedisTemplate reactivePersistedRedisTemplate,
			WebClient.Builder webClientBuilder,
			ProcessorHealthMonitor healthMonitor) {
		this.queuedRedisTemplate = queuedRedisTemplate;
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

		return reactivePersistedRedisTemplate.execute(
				PERSIST_PAYMENT_SCRIPT,
				List.of(PAYMENTS_AMOUNT_TS_KEY + ":" + processorKey, PAYMENTS_COUNT_TS_KEY + ":" + processorKey),
				List.of(
						String.valueOf(paymentSent.getRequestedAt().toEpochMilli()),
						String.valueOf(paymentSent.getAmount().movePointRight(2).longValue())
				)
		).next();
	}

	public Mono<PaymentsSummaryResponse> getPaymentsSummary(String from, String to) {

		return reactivePersistedRedisTemplate.execute(
				GET_SUMMARY_SCRIPT,
				List.of(
						PAYMENTS_AMOUNT_TS_KEY + ":default",
						PAYMENTS_COUNT_TS_KEY + ":default",
						PAYMENTS_AMOUNT_TS_KEY + ":fallback",
						PAYMENTS_COUNT_TS_KEY + ":fallback"
				),
				List.of((from != null) ? String.valueOf(Instant.parse(from).toEpochMilli()) : "-", (to != null) ? String.valueOf(Instant.parse(to).toEpochMilli()) : "+")
		).collectList().map(results -> {

			List<?> summaryList = (List<?>) results.getFirst();

			long defaultCount = Long.parseLong(String.valueOf(summaryList.get(0)));
			long defaultAmountCents = Long.parseLong(String.valueOf(summaryList.get(1)));
			long fallbackCount = Long.parseLong(String.valueOf(summaryList.get(2)));
			long fallbackAmountCents = Long.parseLong(String.valueOf(summaryList.get(3)));

			Summary defaultSummary = new Summary(
					defaultCount,
					BigDecimal.valueOf(defaultAmountCents).scaleByPowerOfTen(-2)
			);
			Summary fallbackSummary = new Summary(
					fallbackCount,
					BigDecimal.valueOf(fallbackAmountCents).scaleByPowerOfTen(-2)
			);
			return new PaymentsSummaryResponse(defaultSummary, fallbackSummary);
		});
	}
}
