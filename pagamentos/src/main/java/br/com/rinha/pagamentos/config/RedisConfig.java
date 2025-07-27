package br.com.rinha.pagamentos.config;

import br.com.rinha.pagamentos.model.QueuedPayment;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import java.math.BigDecimal;

@Configuration
public class RedisConfig {

	abstract static class QueuedPaymentMixIn {
		@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
		abstract BigDecimal getAmount();
	}

	@Bean
	public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, Object> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);

		StringRedisSerializer stringSerializer = new StringRedisSerializer();
		template.setKeySerializer(stringSerializer);
		template.setHashKeySerializer(stringSerializer);

		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());

		objectMapper.addMixIn(QueuedPayment.class, QueuedPaymentMixIn.class);

		PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
				.allowIfSubType("br.com.rinha.pagamentos.model")
				.build();

		objectMapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL);

		Jackson2JsonRedisSerializer<Object> jsonSerializer = new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);

		template.setValueSerializer(jsonSerializer);
		template.setHashValueSerializer(jsonSerializer);
		template.setDefaultSerializer(jsonSerializer);

		template.afterPropertiesSet();

		return template;
	}
}
