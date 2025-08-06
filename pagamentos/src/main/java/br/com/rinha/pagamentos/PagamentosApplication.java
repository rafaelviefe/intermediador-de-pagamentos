package br.com.rinha.pagamentos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import br.com.rinha.pagamentos.config.NativeHints;

@EnableAsync
@EnableScheduling
@SpringBootApplication
@ImportRuntimeHints(NativeHints.class)
public class PagamentosApplication {

	public static void main(String[] args) {
		SpringApplication.run(PagamentosApplication.class, args);
	}

}
