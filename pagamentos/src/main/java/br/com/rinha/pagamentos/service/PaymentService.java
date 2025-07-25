package br.com.rinha.pagamentos.service;

import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.model.PaymentSent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Service
public class PaymentService {

	private static final String HEALTH_FAILING_PREFIX = "health:failing:";
	private static final String RETRY_QUEUE_KEY = "payments:retry-queue";
	private static final String PROCESSED_PAYMENTS_TIMESERIES_KEY = "payments:processed:ts";
	private static final int RETRY_THRESHOLD_FOR_FALLBACK = 3;

	private final RedisTemplate<String, Object> redisTemplate;
	private final RestTemplate restTemplate = new RestTemplate();

	@Value("${processor.default.payments.url}")
	private String processorDefaultUrl;

	@Value("${processor.fallback.payments.url}")
	private String processorFallbackUrl;

	public PaymentService(RedisTemplate<String, Object> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	@Async("virtualThreadExecutor")
	public void processPayment(QueuedPayment payment) {
		try {
			if (IsProcessorAvailable("default")) {
				trySendToProcessor("default", processorDefaultUrl, payment);
				return;
			}

			if (payment.getRetries() > RETRY_THRESHOLD_FOR_FALLBACK) {
				if (IsProcessorAvailable("fallback")) {
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
		String healthKey = HEALTH_FAILING_PREFIX + processorKey;
		redisTemplate.opsForValue().set(healthKey, "1", Duration.ofSeconds(30));
	}

	private void requeuePayment(QueuedPayment payment) {
		payment.setRetries(payment.getRetries() + 1);
		redisTemplate.opsForList().leftPush(RETRY_QUEUE_KEY, payment);
	}

	private void persistSuccessfulPayment(PaymentSent paymentSent, String processorKey) {
		long timestamp = paymentSent.getRequestedAt().toEpochMilli();
		double amount = paymentSent.getAmount().doubleValue();

		redisTemplate.execute((RedisCallback<Object>) connection ->
				connection.execute("TS.ADD",
						PROCESSED_PAYMENTS_TIMESERIES_KEY.getBytes(StandardCharsets.UTF_8),
						String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8),
						String.valueOf(amount).getBytes(StandardCharsets.UTF_8)
				)
		);

		String healthKey = HEALTH_FAILING_PREFIX + processorKey;
		redisTemplate.delete(healthKey);
	}

	private boolean IsProcessorAvailable(String processorKey) {
		return !redisTemplate.hasKey(HEALTH_FAILING_PREFIX + processorKey);
	}
}
