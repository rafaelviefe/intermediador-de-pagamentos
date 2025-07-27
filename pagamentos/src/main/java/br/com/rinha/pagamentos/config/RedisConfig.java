package br.com.rinha.pagamentos.config;

import br.com.rinha.pagamentos.model.QueuedPayment;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

	@Bean
	@Primary
	@Qualifier("paymentRedisTemplate")
	public RedisTemplate<String, QueuedPayment> paymentRedisTemplate(RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, QueuedPayment> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);

		KyroRedisSerializer kryoSerializer = new KyroRedisSerializer();

		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(kryoSerializer);
		template.setHashKeySerializer(new StringRedisSerializer());
		template.setHashValueSerializer(kryoSerializer);
		template.afterPropertiesSet();
		return template;
	}

	@Bean
	@Qualifier("healthCheckRedisTemplate")
	public RedisTemplate<String, String> healthCheckRedisTemplate(RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, String> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new StringRedisSerializer());
		template.afterPropertiesSet();
		return template;
	}
}
