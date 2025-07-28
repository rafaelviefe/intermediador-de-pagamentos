package br.com.rinha.pagamentos.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import br.com.rinha.pagamentos.model.PaymentsSummaryResponse;
import br.com.rinha.pagamentos.service.PaymentService;

@RestController
@RequestMapping("/payments-summary")
public class SummaryController {

	private final PaymentService paymentService;

	public SummaryController(PaymentService paymentService) {
		this.paymentService = paymentService;
	}

	@GetMapping
	public ResponseEntity<PaymentsSummaryResponse> getSummary(
			@RequestParam(required = false) String from,
			@RequestParam(required = false) String to) {
		return ResponseEntity.ok(paymentService.getPaymentsSummary(from, to));
	}

}
