package br.com.rinha.pagamentos.service;

import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.model.PaymentSent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Service
public class PaymentService {

	private static final String HEALTH_FAILING_PREFIX = "health:failing:";
	private static final String RETRY_QUEUE_KEY = "payments:retry-queue";
	private static final String PROCESSED_PAYMENTS_TIMESERIES_KEY = "payments:processed:ts";
	private static final int RETRY_THRESHOLD_FOR_FALLBACK = 3;

	private static final RedisScript<Long> PERSIST_PAYMENT_SCRIPT =
			new DefaultRedisScript<>(
					"redis.call('TS.ADD', KEYS[1], ARGV[1], ARGV[2], 'LABELS', 'processor', ARGV[3]); " +
							"return redis.call('DEL', KEYS[2]);",
					Long.class
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
		try {
			PaymentSent paymentSent = new PaymentSent(payment);
			ResponseEntity<String> response = restTemplate.postForEntity(url, paymentSent, String.class);

			if (response.getStatusCode().is2xxSuccessful()) {
				persistSuccessfulPayment(paymentSent, processorKey);
			} else {
				handleFailedAttempt(processorKey);
				requeuePayment(payment);
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
				.multiply(new BigDecimal("100"))
				.longValue();

		healthCheckRedisTemplate.execute((RedisCallback<Object>) connection -> connection.scriptingCommands().eval(
				PERSIST_PAYMENT_SCRIPT.getScriptAsString().getBytes(StandardCharsets.UTF_8),
				ReturnType.INTEGER,
				2,
				PROCESSED_PAYMENTS_TIMESERIES_KEY.getBytes(StandardCharsets.UTF_8),
				(HEALTH_FAILING_PREFIX + processorKey).getBytes(StandardCharsets.UTF_8),
				String.valueOf(paymentSent.getRequestedAt().toEpochMilli()).getBytes(StandardCharsets.UTF_8),
				String.valueOf(amountInCents).getBytes(StandardCharsets.UTF_8),
				processorKey.getBytes(StandardCharsets.UTF_8)
		));
	}

	private boolean isProcessorAvailable(String processorKey) {
		return healthCheckRedisTemplate.hasKey(HEALTH_FAILING_PREFIX + processorKey) != Boolean.TRUE;
	}
}
