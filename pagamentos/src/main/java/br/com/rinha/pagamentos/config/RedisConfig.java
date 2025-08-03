package br.com.rinha.pagamentos.config;

import br.com.rinha.pagamentos.health.ProcessorHealthMonitor;
import br.com.rinha.pagamentos.model.QueuedPayment;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.nio.charset.StandardCharsets;

@Configuration
public class RedisConfig {

	@Bean
	@Primary
	@Qualifier("reactiveQueuedRedisTemplate")
	public ReactiveRedisTemplate<String, QueuedPayment> reactiveQueuedRedisTemplate(
			ReactiveRedisConnectionFactory factory) {

		var kryoSerializer = new KyroRedisSerializer();
		var stringSerializer = new StringRedisSerializer();

		RedisSerializationContext<String, QueuedPayment> serializationContext = RedisSerializationContext
				.<String, QueuedPayment>newSerializationContext(stringSerializer)
				.key(stringSerializer)
				.value((RedisSerializer<QueuedPayment>) (RedisSerializer<?>) kryoSerializer)
				.hashKey(stringSerializer)
				.hashValue((RedisSerializer<QueuedPayment>) (RedisSerializer<?>) kryoSerializer)
				.build();

		return new ReactiveRedisTemplate<>(factory, serializationContext);
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
				createTimeSeriesIfNotExists(connection, "payments:amount:ts:default", "type", "amount", "processor", "default");
				createTimeSeriesIfNotExists(connection, "payments:count:ts:default", "type", "count", "processor", "default");
				createTimeSeriesIfNotExists(connection, "payments:amount:ts:fallback", "type", "amount", "processor", "fallback");
				createTimeSeriesIfNotExists(connection, "payments:count:ts:fallback", "type", "count", "processor", "fallback");
				return null;
			});
		};
	}

	@Bean
	public ReactiveStringRedisTemplate reactivePersistedRedisTemplate(ReactiveRedisConnectionFactory factory) {
		return new ReactiveStringRedisTemplate(factory);
	}

	@Bean
	public RedisMessageListenerContainer redisMessageListenerContainer(
			RedisConnectionFactory connectionFactory,
			ProcessorHealthMonitor healthMonitor) {

		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.addMessageListener(healthMonitor, healthMonitor.getTopic());
		return container;
	}

	private static final String CREATE_TS_IF_NOT_EXISTS_SCRIPT =
			"if redis.call('EXISTS', KEYS[1]) == 0 then " +
					"  return redis.call('TS.CREATE', KEYS[1], 'DUPLICATE_POLICY', 'SUM', 'LABELS', ARGV[1], ARGV[2], ARGV[3], ARGV[4]) " +
					"else " +
					"  return 'OK' " +
					"end";

	private void createTimeSeriesIfNotExists(RedisConnection connection, String key, String label1Name, String label1Value, String label2Name, String label2Value) {
		connection.scriptingCommands().eval(
				CREATE_TS_IF_NOT_EXISTS_SCRIPT.getBytes(StandardCharsets.UTF_8),
				ReturnType.STATUS,
				1,
				key.getBytes(StandardCharsets.UTF_8),
				label1Name.getBytes(StandardCharsets.UTF_8),
				label1Value.getBytes(StandardCharsets.UTF_8),
				label2Name.getBytes(StandardCharsets.UTF_8),
				label2Value.getBytes(StandardCharsets.UTF_8)
		);
	}
}
