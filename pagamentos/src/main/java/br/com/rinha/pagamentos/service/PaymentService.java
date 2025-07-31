package br.com.rinha.pagamentos.service;

import br.com.rinha.pagamentos.model.PaymentsSummaryResponse;
import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.model.PaymentSent;
import br.com.rinha.pagamentos.model.Summary;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Service
public class PaymentService {

	private static final String HEALTH_FAILING_PREFIX = "health:failing:";
	private static final String RETRY_QUEUE_KEY = "payments:retry-queue";
	private static final String PROCESSED_PAYMENTS_TIMESERIES_KEY = "payments:processed:ts";
	private static final int RETRY_THRESHOLD_FOR_FALLBACK = 3;
	private static final BigDecimal ONE_HUNDRED = new BigDecimal("100");

	private static final RedisScript<Long> PERSIST_PAYMENT_SCRIPT =
			new DefaultRedisScript<>(
					"""
					redis.call('TS.ADD', KEYS[1], '*', ARGV[1]);
					redis.call('DEL', KEYS[2]);
					return 1
					""",
					Long.class
			);

	private static final RedisScript<List> GET_SUMMARY_SCRIPT =
			new DefaultRedisScript<>(
					"""
					local timeBucket = 9999999999999
					local summary = {0, 0, 0, 0}
					
					if redis.call('EXISTS', KEYS[1]) == 1 then
					   local default_sum_res = redis.call('TS.RANGE', KEYS[1], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					   local default_count_res = redis.call('TS.RANGE', KEYS[1], ARGV[1], ARGV[2], 'AGGREGATION', 'count', timeBucket)
					   if #default_count_res > 0 then summary[1] = default_count_res[1][2] end
					   if #default_sum_res > 0 then summary[2] = default_sum_res[1][2] end
					end
					
					if redis.call('EXISTS', KEYS[2]) == 1 then
					   local fallback_sum_res = redis.call('TS.RANGE', KEYS[2], ARGV[1], ARGV[2], 'AGGREGATION', 'sum', timeBucket)
					   local fallback_count_res = redis.call('TS.RANGE', KEYS[2], ARGV[1], ARGV[2], 'AGGREGATION', 'count', timeBucket)
					   if #fallback_count_res > 0 then summary[3] = fallback_count_res[1][2] end
					   if #fallback_sum_res > 0 then summary[4] = fallback_sum_res[1][2] end
					end
					
					return summary
					""",
					List.class
			);

	private final RedisTemplate<String, QueuedPayment> paymentRedisTemplate;
	private final RedisTemplate<String, String> healthCheckRedisTemplate;
	private final RestTemplate restTemplate;

	@Value("${processor.default.payments.url}")
	private String processorDefaultUrl;

	@Value("${processor.fallback.payments.url}")
	private String processorFallbackUrl;

	public PaymentService(
			@Qualifier("paymentRedisTemplate") RedisTemplate<String, QueuedPayment> paymentRedisTemplate,
			@Qualifier("healthCheckRedisTemplate") RedisTemplate<String, String> healthCheckRedisTemplate,
			RestTemplate restTemplate) {
		this.paymentRedisTemplate = paymentRedisTemplate;
		this.healthCheckRedisTemplate = healthCheckRedisTemplate;
		this.restTemplate = restTemplate;
	}

	@Async("virtualThreadExecutor")
	public void processPayment(QueuedPayment payment) {
		try {
			if (isProcessorAvailable("default")) {
				trySendToProcessor("default", processorDefaultUrl, payment);
				return;
			}
			if (payment.getRetries() > RETRY_THRESHOLD_FOR_FALLBACK) {
				if (isProcessorAvailable("fallback")) {
					trySendToProcessor("fallback", processorFallbackUrl, payment);
					return;
				}
			}
			requeuePayment(payment);
		} catch (Exception e) {
			requeuePayment(payment);
		}
	}

	private void trySendToProcessor(String processorKey, String url, QueuedPayment payment) {
		PaymentSent paymentSent = new PaymentSent(payment);
		try {
			restTemplate.postForEntity(url, paymentSent, String.class);
			persistSuccessfulPayment(paymentSent, processorKey);

		} catch (RestClientResponseException e) {
			if (e.getStatusCode().is5xxServerError()) {
				handleFailedAttempt(processorKey);
				requeuePayment(payment);
			} else {
				persistSuccessfulPayment(paymentSent, processorKey);
			}
		} catch (RestClientException e) {
			handleFailedAttempt(processorKey);
			requeuePayment(payment);
		}
	}

	private void handleFailedAttempt(String processorKey) {
		healthCheckRedisTemplate.opsForValue().set(HEALTH_FAILING_PREFIX + processorKey, "1", Duration.ofSeconds(5));
	}

	private void requeuePayment(QueuedPayment payment) {
		payment.setRetries(payment.getRetries() + 1);
		paymentRedisTemplate.opsForList().leftPush(RETRY_QUEUE_KEY, payment);
	}

	private void persistSuccessfulPayment(PaymentSent paymentSent, String processorKey) {
		long amountInCents = paymentSent.getAmount()
				.multiply(ONE_HUNDRED)
				.longValue();

		healthCheckRedisTemplate.execute((RedisCallback<Object>) connection -> connection.scriptingCommands().eval(
				PERSIST_PAYMENT_SCRIPT.getScriptAsString().getBytes(StandardCharsets.UTF_8),
				ReturnType.INTEGER,
				2,
				(PROCESSED_PAYMENTS_TIMESERIES_KEY + ":" + processorKey).getBytes(StandardCharsets.UTF_8),
				(HEALTH_FAILING_PREFIX + processorKey).getBytes(StandardCharsets.UTF_8),
				String.valueOf(amountInCents).getBytes(StandardCharsets.UTF_8)
		));
	}

	private boolean isProcessorAvailable(String processorKey) {
		return healthCheckRedisTemplate.hasKey(HEALTH_FAILING_PREFIX + processorKey) != Boolean.TRUE;
	}

	public PaymentsSummaryResponse getPaymentsSummary(String from, String to) {
		String fromTimestamp = (from != null) ? String.valueOf(Instant.parse(from).toEpochMilli()) : "-";
		String toTimestamp = (to != null) ? String.valueOf(Instant.parse(to).toEpochMilli()) : "+";
		String defaultKey = PROCESSED_PAYMENTS_TIMESERIES_KEY + ":default";
		String fallbackKey = PROCESSED_PAYMENTS_TIMESERIES_KEY + ":fallback";

		List<Object> results = Optional.ofNullable(
				healthCheckRedisTemplate.execute(
						GET_SUMMARY_SCRIPT,
						List.of(defaultKey, fallbackKey),
						fromTimestamp,
						toTimestamp
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
