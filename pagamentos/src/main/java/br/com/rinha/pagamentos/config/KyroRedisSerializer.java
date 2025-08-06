	package br.com.rinha.pagamentos.config;

	import com.esotericsoftware.kryo.Kryo;
	import com.esotericsoftware.kryo.io.Input;
	import com.esotericsoftware.kryo.io.Output;
	import com.esotericsoftware.kryo.serializers.DefaultSerializers;
	import de.javakaffee.kryoserializers.UUIDSerializer;
	import org.objenesis.strategy.StdInstantiatorStrategy;
	import org.springframework.data.redis.serializer.RedisSerializer;
	import org.springframework.data.redis.serializer.SerializationException;
	import java.io.ByteArrayOutputStream;
	import java.util.UUID;

	public class KyroRedisSerializer implements RedisSerializer<Object> {

		private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
			Kryo kryo = new Kryo();

			kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

			kryo.register(br.com.rinha.pagamentos.model.QueuedPayment.class, 10);
			kryo.register(java.math.BigDecimal.class, new DefaultSerializers.BigDecimalSerializer(), 11);
			kryo.register(UUID.class, new UUIDSerializer(), 12);
			return kryo;
		});

		@Override
		public byte[] serialize(Object o) throws SerializationException {
			if (o == null) {
				return new byte[0];
			}
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
					Output output = new Output(baos)) {
				kryoThreadLocal.get().writeClassAndObject(output, o);
				output.flush();
				return baos.toByteArray();
			} catch (Exception e) {
				throw new SerializationException("Could not serialize object with Kryo", e);
			}
		}

		@Override
		public Object deserialize(byte[] bytes) throws SerializationException {
			if (bytes == null || bytes.length == 0) {
				return null;
			}
			try (Input input = new Input(bytes)) {
				return kryoThreadLocal.get().readClassAndObject(input);
			} catch (Exception e) {
				throw new SerializationException("Could not deserialize object with Kryo", e);
			}
		}
	}
