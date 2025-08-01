package br.com.rinha.pagamentos.config;

import br.com.rinha.pagamentos.model.QueuedPayment;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import java.nio.charset.StandardCharsets;

@Configuration
public class RedisConfig {

	@Bean
	@Primary
	@Qualifier("queuedRedisTemplate")
	public RedisTemplate<String, QueuedPayment> queuedRedisTemplate(RedisConnectionFactory connectionFactory) {
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
	@Qualifier("persistedRedisTemplate")
	public RedisTemplate<String, String> persistedRedisTemplate(RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, String> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new StringRedisSerializer());
		template.afterPropertiesSet();
		return template;
	}

	@Bean
	public ApplicationRunner redisTimeSeriesInitializer(@Qualifier("persistedRedisTemplate") RedisTemplate<String, String> redisTemplate) {
		return args -> {
			redisTemplate.execute((RedisConnection connection) -> {
				createTimeSeriesIfNotExists(connection, "payments:amount:ts:default");
				createTimeSeriesIfNotExists(connection, "payments:count:ts:default");
				createTimeSeriesIfNotExists(connection, "payments:amount:ts:fallback");
				createTimeSeriesIfNotExists(connection, "payments:count:ts:fallback");
				return null;
			});
		};
	}

	private void createTimeSeriesIfNotExists(RedisConnection connection, String key) {
		byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
		if (connection.keyCommands().exists(keyBytes) == Boolean.FALSE) {
			connection.execute("TS.CREATE", keyBytes, "DUPLICATE_POLICY".getBytes(StandardCharsets.UTF_8), "SUM".getBytes(StandardCharsets.UTF_8));
		}
	}
}
