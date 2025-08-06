package br.com.rinha.pagamentos.config;

import br.com.rinha.pagamentos.health.ProcessorHealthMonitor;
import br.com.rinha.pagamentos.model.HealthCheckResponse;
import br.com.rinha.pagamentos.model.PaymentSent;
import br.com.rinha.pagamentos.model.PaymentsSummaryResponse;
import br.com.rinha.pagamentos.model.QueuedPayment;
import br.com.rinha.pagamentos.model.Summary;
import de.javakaffee.kryoserializers.UUIDSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.redis.connection.MessageListener;

import java.math.BigDecimal;
import java.util.UUID;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;

public class NativeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
		var reflectionCategories = MemberCategory.values();

		hints.reflection().registerTypes(
				TypeReference.listOf(
						QueuedPayment.class,
						BigDecimal.class,
						UUID.class,
						UUIDSerializer.class,
						HealthCheckResponse.class,
						PaymentSent.class,
						PaymentsSummaryResponse.class,
						Summary.class,
						DefaultSerializers.BigDecimalSerializer.class,
						StdInstantiatorStrategy.class
				),
				hint -> hint.withMembers(reflectionCategories)
		);

		hints.proxies().registerJdkProxy(
				TypeReference.of(ProcessorHealthMonitor.class),
				TypeReference.of(MessageListener.class),
				TypeReference.of("org.springframework.aop.SpringProxy"),
				TypeReference.of("org.springframework.aop.framework.Advised"),
				TypeReference.of("org.springframework.core.DecoratingProxy")
		);

		TypeReference.listOf(
				QueuedPayment.class,
				BigDecimal.class,
				UUID.class
		).forEach(type -> hints.serialization().registerType(type));
	}
}
