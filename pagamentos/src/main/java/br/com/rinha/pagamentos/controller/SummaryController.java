package br.com.rinha.pagamentos.controller;

import java.time.Duration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import br.com.rinha.pagamentos.model.PaymentsSummaryResponse;
import br.com.rinha.pagamentos.service.PaymentService;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/payments-summary")
public class SummaryController {

	private final PaymentService paymentService;

	public SummaryController(PaymentService paymentService) {
		this.paymentService = paymentService;
	}

	@GetMapping
	public Mono<ResponseEntity<PaymentsSummaryResponse>> getSummary(
			@RequestParam(required = false) String from,
			@RequestParam(required = false) String to) {

		return paymentService.getPaymentsSummary(from, to)
				.delaySubscription(Duration.ofMillis(1111))
				.map(ResponseEntity::ok);
	}
}
