package io.rcardin.spring.kafka.stream;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.time.Duration;
import java.util.Map;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public MessageConverter jsonMessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	@Bean
	public Queue wordsQueue() {
		return new Queue("words", false);
	}

	@Bean
	public Queue wordCountersQueue() {
		return new Queue("word-counters", false);
	}

	@Bean
	public TopicExchange exchange() {
		return new TopicExchange("app.topic");
	}

	@Bean
	public Binding binding(Queue wordsQueue, TopicExchange exchange) {
		return BindingBuilder.bind(wordsQueue).to(exchange).with("words");
	}

	@Bean
	public Binding binding2(Queue wordCountersQueue, TopicExchange exchange) {
		return BindingBuilder.bind(wordCountersQueue).to(exchange).with("word-counters");
	}

	@Bean
	public RedisCacheManager cacheManager(StringRedisTemplate redisTemplate) {
		RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
				.serializeKeysWith(RedisSerializationContext.SerializationPair.fromSerializer(new StringRedisSerializer()))
				.serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(new GenericJackson2JsonRedisSerializer()))
				.entryTtl(Duration.ofMinutes(10));

		return RedisCacheManager.builder(redisTemplate.getConnectionFactory())
				.withInitialCacheConfigurations(Map.of("wordCount", config))
				.build();
	}
}

@Service
class WordCountService {

	private final RabbitTemplate rabbitTemplate;
	private final WordCountRepository wordCountRepository;
	private final StringRedisTemplate redisTemplate;
	private final JdbcTemplate jdbcTemplate;

	public WordCountService(RabbitTemplate rabbitTemplate, WordCountRepository wordCountRepository, StringRedisTemplate redisTemplate, JdbcTemplate jdbcTemplate) {
		this.rabbitTemplate = rabbitTemplate;
		this.wordCountRepository = wordCountRepository;
		this.redisTemplate = redisTemplate;
		this.jdbcTemplate = jdbcTemplate;
	}

	public void processWords(String message) {
		String[] words = message.split("\\s+");
		for (String word : words) {
			String cacheKey = "wordCount:" + word;
			
			// Try to get from cache first
			String cachedCount = redisTemplate.opsForValue().get(cacheKey);
			Long count;
			
			if (cachedCount != null) {
				count = Long.parseLong(cachedCount);
			} else {
				// Get from database if not in cache
				count = wordCountRepository.findById(word)
						.map(WordCount::getCount)
						.orElse(0L);
			}
			
			long newCount = count + 1;
			
			// Update both database and cache
			wordCountRepository.save(new WordCount(word, newCount));
			redisTemplate.opsForValue().set(cacheKey, String.valueOf(newCount));
			
			// Send to RabbitMQ
			rabbitTemplate.convertAndSend("word-counters", new WordCount(word, newCount));
		}
	}
}

@Entity
class WordCount {

	@Id
	private String word;
	private Long count;

	public WordCount() {
	}

	public WordCount(String word, Long count) {
		this.word = word;
		this.count = count;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}
}

interface WordCountRepository extends JpaRepository<WordCount, String> {
}