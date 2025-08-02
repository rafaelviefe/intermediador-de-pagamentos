package br.com.rinha.pagamentos.controller;

import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
public class PaymentController {

	private final PaymentService paymentService;

	public PaymentController(PaymentService paymentService) {
		this.paymentService = paymentService;
	}

	@PostMapping
	public ResponseEntity<Void> createPayment(@RequestBody QueuedPayment request) {

		paymentService.handlePayment(request);

		return ResponseEntity.noContent().build();
	}

}
