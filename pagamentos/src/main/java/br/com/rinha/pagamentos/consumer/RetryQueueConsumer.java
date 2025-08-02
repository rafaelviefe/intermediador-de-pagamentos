package br.com.rinha.pagamentos.consumer;

import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.service.PaymentService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class RetryQueueConsumer implements ApplicationListener<ApplicationReadyEvent> {

	private static final String PROCESSING_QUEUE_KEY = "payments:processing-queue";
	private static final long BLOCKING_TIMEOUT_SECONDS = 5;
	private static final int BATCH_SIZE = 50;

	private final ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

	private final RedisTemplate<String, QueuedPayment> redisTemplate;
	private final PaymentService paymentService;

	@Value("${retry.consumer.concurrency}")
	private int concurrencyLevel;

	public RetryQueueConsumer(RedisTemplate<String, QueuedPayment> redisTemplate, PaymentService paymentService) {
		this.redisTemplate = redisTemplate;
		this.paymentService = paymentService;
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		for (int i = 0; i < concurrencyLevel; i++) {
			virtualThreadExecutor.submit(this::consumeQueueInBatches);
		}
	}

	private void consumeQueueInBatches() {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				QueuedPayment firstPayment = redisTemplate.opsForList().rightPop(
						PROCESSING_QUEUE_KEY,
						Duration.ofSeconds(BLOCKING_TIMEOUT_SECONDS)
				);

				if (firstPayment == null) {
					continue;
				}

				List<QueuedPayment> remainingPayments = redisTemplate.opsForList().rightPop(PROCESSING_QUEUE_KEY, BATCH_SIZE - 1);

				Flux.fromIterable(remainingPayments)
						.startWith(firstPayment)
						.parallel()
						.runOn(Schedulers.boundedElastic())
						.doOnNext(paymentService::processPayment)
						.sequential()
						.blockLast();

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
