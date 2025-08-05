package br.com.rinha.pagamentos.consumer;

import br.com.rinha.pagamentos.health.ProcessorHealthMonitor;
import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.service.PaymentService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

@Service
public class RetryQueueConsumer implements ApplicationListener<ApplicationReadyEvent> {

	private static final String PROCESSING_QUEUE_KEY = "payments:processing-queue";
	private static final Duration PROCESSORS_UNAVAILABLE_DELAY = Duration.ofMillis(100);

	private final ReactiveRedisTemplate<String, QueuedPayment> reactiveRedisTemplate;
	private final PaymentService paymentService;
	private final ProcessorHealthMonitor processorHealthMonitor;

	@Value("${retry.consumer.concurrency}")
	private int concurrencyLevel;

	public RetryQueueConsumer(
			@Qualifier("reactiveQueuedRedisTemplate") ReactiveRedisTemplate<String, QueuedPayment> reactiveRedisTemplate,
			PaymentService paymentService,
			ProcessorHealthMonitor processorHealthMonitor) {
		this.reactiveRedisTemplate = reactiveRedisTemplate;
		this.paymentService = paymentService;
		this.processorHealthMonitor = processorHealthMonitor;
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		this.consumeFromQueue()
				.subscribeOn(Schedulers.parallel())
				.subscribe();
	}

	private Mono<Void> consumeFromQueue() {
		return Flux.defer(() -> {
					boolean canProcess = processorHealthMonitor.isDefaultProcessorAvailable() || processorHealthMonitor.isFallbackProcessorAvailable();

					if (canProcess) {
						return reactiveRedisTemplate.opsForList()
								.rightPop(PROCESSING_QUEUE_KEY, Duration.ofSeconds(1));
					} else {
						return Flux.<QueuedPayment>empty().delaySubscription(PROCESSORS_UNAVAILABLE_DELAY);
					}
				})
				.repeat()
				.parallel(concurrencyLevel)
				.runOn(Schedulers.parallel())
				.flatMap(payment -> Mono.fromRunnable(() -> paymentService.processPayment(payment)))
				.sequential()
				.then();
	}
}
