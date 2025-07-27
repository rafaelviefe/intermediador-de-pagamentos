package br.com.rinha.pagamentos.consumer;

import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.service.PaymentService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class RetryQueueConsumer implements ApplicationListener<ApplicationReadyEvent> {

	private static final String RETRY_QUEUE_KEY = "payments:retry-queue";
	private static final long BLOCKING_TIMEOUT_SECONDS = 5;

	private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

	private final RedisTemplate<String, Object> redisTemplate;
	private final PaymentService paymentService;

	@Value("${retry.consumer.concurrency:10}")
	private int concurrencyLevel;

	public RetryQueueConsumer(RedisTemplate<String, Object> redisTemplate, PaymentService paymentService) {
		this.redisTemplate = redisTemplate;
		this.paymentService = paymentService;
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		for (int i = 0; i < concurrencyLevel; i++) {
			virtualThreadExecutor.submit(this::consumeQueue);
		}
	}

	private void consumeQueue() {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				Object paymentObject = redisTemplate.opsForList().rightPop(
						RETRY_QUEUE_KEY,
						Duration.ofSeconds(BLOCKING_TIMEOUT_SECONDS)
				);

				if (paymentObject instanceof QueuedPayment payment) {
					paymentService.processPayment(payment);
				}

			} catch (Exception e) {
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException interruptedException) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
}
