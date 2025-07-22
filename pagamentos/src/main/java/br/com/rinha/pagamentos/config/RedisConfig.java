package br.com.rinha.pagamentos.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import br.com.rinha.pagamentos.model.ProcessorHealthStatus;

@Configuration
public class RedisConfig {
	@Bean
	public RedisTemplate<String, ProcessorHealthStatus> redisTemplate(RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, ProcessorHealthStatus> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new Jackson2JsonRedisSerializer<>(ProcessorHealthStatus.class));
		return template;
	}
}
