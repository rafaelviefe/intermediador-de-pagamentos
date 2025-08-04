package br.com.rinha.pagamentos.service;

import br.com.rinha.pagamentos.health.ProcessorHealthMonitor;
import br.com.rinha.pagamentos.model.PaymentsSummaryResponse;
import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.model.PaymentSent;
import br.com.rinha.pagamentos.model.Summary;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
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

	private static final RedisScript<List> GENERIC_COMMAND_SCRIPT =
			new DefaultRedisScript<>("return redis.call(unpack(ARGV))", List.class);

	private final ReactiveRedisTemplate<String, QueuedPayment> reactiveQueuedRedisTemplate;
	private final ReactiveStringRedisTemplate reactivePersistedRedisTemplate;
	private final WebClient webClient;
	private final ProcessorHealthMonitor healthMonitor;

	@Value("${processor.default.payments.url}")
	private String processorDefaultUrl;
	@Value("${processor.fallback.payments.url}")
	private String processorFallbackUrl;

	public PaymentService(
			@Qualifier("reactiveQueuedRedisTemplate") ReactiveRedisTemplate<String, QueuedPayment> reactiveQueuedRedisTemplate,
			@Qualifier("reactivePersistedRedisTemplate") ReactiveStringRedisTemplate reactivePersistedRedisTemplate,
			WebClient.Builder webClientBuilder,
			ProcessorHealthMonitor healthMonitor) {
		this.reactiveQueuedRedisTemplate = reactiveQueuedRedisTemplate;
		this.reactivePersistedRedisTemplate = reactivePersistedRedisTemplate;
		this.webClient = webClientBuilder.build();
		this.healthMonitor = healthMonitor;
	}

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
			queuePayment(payment).subscribe();
		}
	}

	public Mono<Long> queuePayment(QueuedPayment payment) {
		return reactiveQueuedRedisTemplate.opsForList().leftPush(PROCESSING_QUEUE_KEY, payment);
	}

	private Mono<Boolean> requeue(QueuedPayment payment) {
		return this.queuePayment(payment).thenReturn(false);
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

		List<String> commandAndArgs = getRedisData(from, to);

		return reactivePersistedRedisTemplate.execute(
				GENERIC_COMMAND_SCRIPT,
				List.of(),
				commandAndArgs
		).collectList().map(results -> {
			List<?> rawResultList = (List<?>) results.get(0);
			return parseMRangeResponse(rawResultList);
		});
	}

	private static List<String> getRedisData(String from, String to) {
		List<String> commandAndArgs = new ArrayList<>(9);
		commandAndArgs.add("TS.MRANGE");
		commandAndArgs.add((from != null) ? String.valueOf(Instant.parse(from).toEpochMilli()) : "-");
		commandAndArgs.add((to != null) ? String.valueOf(Instant.parse(to).toEpochMilli()) : "+");
		commandAndArgs.add("WITHLABELS");
		commandAndArgs.add("AGGREGATION");
		commandAndArgs.add("sum");
		commandAndArgs.add("9999999999999");
		commandAndArgs.add("FILTER");
		commandAndArgs.add("processor=(default,fallback)");
		return commandAndArgs;
	}

	private PaymentsSummaryResponse parseMRangeResponse(List<?> rawResult) {

		long defaultCount = 0;
		long defaultAmountCents = 0;
		long fallbackCount = 0;
		long fallbackAmountCents = 0;

		for (Object seriesDataObject : rawResult) {
			List<?> seriesData = (List<?>) seriesDataObject;
			List<?> labelsList = (List<?>) seriesData.get(1);

			String processor = "";
			String type = "";

			for (Object labelPairObject : labelsList) {
				List<?> labelPair = (List<?>) labelPairObject;
				String labelName = (String) labelPair.get(0);
				String labelValue = (String) labelPair.get(1);
				if ("processor".equals(labelName)) {
					processor = labelValue;
				} else if ("type".equals(labelName)) {
					type = labelValue;
				}
			}

			List<?> dataPointsList = (List<?>) seriesData.get(2);

			switch (processor) {
			case "default":
				if (!dataPointsList.isEmpty()) {
					long value = Long.parseLong((String) ((List<?>) dataPointsList.get(0)).get(1));
					if ("count".equals(type)) defaultCount = value;
					else defaultAmountCents = value;
				}
				break;
			case "fallback":
				if (!dataPointsList.isEmpty()) {
					long value = Long.parseLong((String) ((List<?>) dataPointsList.get(0)).get(1));
					if ("count".equals(type)) fallbackCount = value;
					else fallbackAmountCents = value;
				}
				break;
			}
		}

		Summary defaultSummary = new Summary(defaultCount, BigDecimal.valueOf(defaultAmountCents, 2));
		Summary fallbackSummary = new Summary(fallbackCount, BigDecimal.valueOf(fallbackAmountCents, 2));

		return new PaymentsSummaryResponse(defaultSummary, fallbackSummary);
	}
}
