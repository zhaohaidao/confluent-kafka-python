import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class AuthScenarioCli {
    private static final int DETAIL_MAX_LEN = 280;

    private static class CliArgs {
        String bootstrap;
        String topic;
        String group;
        boolean useGroup;
        boolean useAuth;
        String username;
        String password;
        String securityProtocol;
        String anonymousSecurityProtocol;
        String saslMechanism;
        int socketTimeoutMs;
        int requestTimeoutMs;
        int assignmentTimeoutMs;
        int consumeTimeoutMs;
        int pollTimeoutMs;
        int sendTimeoutMs;
        String clientIdPrefix;
    }

    private static class OperationResult {
        final boolean ok;
        final String detail;

        OperationResult(boolean ok, String detail) {
            this.ok = ok;
            this.detail = detail;
        }
    }

    private static class ScenarioResult {
        final String groupId;
        final OperationResult produce;
        final OperationResult consume;

        ScenarioResult(String groupId, OperationResult produce, OperationResult consume) {
            this.groupId = groupId;
            this.produce = produce;
            this.consume = consume;
        }
    }

    public static void main(String[] args) {
        try {
            CliArgs cli = parseArgs(args);
            ScenarioResult result = runScenario(cli);
            System.out.println("group_id=" + sanitize(result.groupId));
            System.out.println("produce_ok=" + result.produce.ok);
            System.out.println("produce_detail=" + sanitize(result.produce.detail));
            System.out.println("consume_ok=" + result.consume.ok);
            System.out.println("consume_detail=" + sanitize(result.consume.detail));
            System.exit(0);
        } catch (Exception e) {
            System.out.println("group_id=");
            System.out.println("produce_ok=false");
            System.out.println("produce_detail=runner_error:" + sanitize(shortError(e)));
            System.out.println("consume_ok=false");
            System.out.println("consume_detail=runner_error");
            System.exit(2);
        }
    }

    private static ScenarioResult runScenario(CliArgs cli) {
        String groupId = cli.useGroup ? cli.group : "no-group-" + cli.topic + "-" + uuid8();

        Properties producerProps = buildProducerProps(cli);
        String marker = "java-main-" + cli.topic + "-" + UUID.randomUUID();
        OperationResult produceResult = runProduce(cli, producerProps, marker);
        if (!produceResult.ok) {
            return new ScenarioResult(groupId, produceResult, new OperationResult(false, "skipped_produce_failed"));
        }

        OperationResult consumeResult = runConsumeWithProbe(cli, groupId, producerProps);
        return new ScenarioResult(groupId, produceResult, consumeResult);
    }

    private static Properties buildProducerProps(CliArgs cli) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cli.bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(cli.requestTimeoutMs));
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Integer.toString(Math.max(1000, cli.requestTimeoutMs)));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Math.max(2000, cli.requestTimeoutMs * 2)));
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("socket.timeout.ms", Integer.toString(cli.socketTimeoutMs));
        props.put("client.id", cli.clientIdPrefix + "-producer-" + uuid8());

        if (cli.useAuth) {
            props.put("security.protocol", cli.securityProtocol);
            props.put("sasl.mechanism", cli.saslMechanism);
            props.put("sasl.jaas.config", buildJaas(cli.username, cli.password, cli.saslMechanism));
        } else if (cli.anonymousSecurityProtocol != null && !cli.anonymousSecurityProtocol.isEmpty()) {
            props.put("security.protocol", cli.anonymousSecurityProtocol);
        }

        return props;
    }

    private static Properties buildConsumerProps(CliArgs cli, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cli.bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(cli.requestTimeoutMs));
        props.put("socket.timeout.ms", Integer.toString(cli.socketTimeoutMs));
        props.put("client.id", cli.clientIdPrefix + "-consumer-" + uuid8());

        if (cli.useAuth) {
            props.put("security.protocol", cli.securityProtocol);
            props.put("sasl.mechanism", cli.saslMechanism);
            props.put("sasl.jaas.config", buildJaas(cli.username, cli.password, cli.saslMechanism));
        } else if (cli.anonymousSecurityProtocol != null && !cli.anonymousSecurityProtocol.isEmpty()) {
            props.put("security.protocol", cli.anonymousSecurityProtocol);
        }

        return props;
    }

    private static OperationResult runProduce(CliArgs cli, Properties producerProps, String marker) {
        try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(cli.topic, marker, marker);
            producer.send(record).get(cli.sendTimeoutMs, TimeUnit.MILLISECONDS);
            producer.flush();
            return new OperationResult(true, "ok");
        } catch (TimeoutException e) {
            return new OperationResult(false, "produce_timeout:" + shortError(e));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return new OperationResult(false, shortError(cause));
        } catch (Exception e) {
            return new OperationResult(false, shortError(e));
        }
    }

    private static OperationResult runConsumeWithProbe(CliArgs cli, String groupId, Properties producerProps) {
        Consumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(buildConsumerProps(cli, groupId));
            consumer.subscribe(Collections.singletonList(cli.topic));

            long assignmentDeadline = System.currentTimeMillis() + cli.assignmentTimeoutMs;
            while (System.currentTimeMillis() < assignmentDeadline) {
                try {
                    consumer.poll(Duration.ofMillis(cli.pollTimeoutMs));
                } catch (AuthenticationException e) {
                    return new OperationResult(false, shortError(e));
                }
                if (!consumer.assignment().isEmpty()) {
                    break;
                }
            }
            if (consumer.assignment().isEmpty()) {
                return new OperationResult(false, "assignment_timeout");
            }

            String probeMarker = "java-probe-" + cli.topic + "-" + UUID.randomUUID();
            OperationResult probeProduce = runProduce(cli, producerProps, probeMarker);
            if (!probeProduce.ok) {
                return new OperationResult(false, "consume_probe_failed:" + probeProduce.detail);
            }

            long consumeDeadline = System.currentTimeMillis() + cli.consumeTimeoutMs;
            while (System.currentTimeMillis() < consumeDeadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(cli.pollTimeoutMs));
                for (ConsumerRecord<String, String> record : records) {
                    if (probeMarker.equals(record.value())) {
                        return new OperationResult(true, "ok");
                    }
                }
            }
            return new OperationResult(false, "consume_timeout");
        } catch (Exception e) {
            return new OperationResult(false, shortError(e));
        } finally {
            if (consumer != null) {
                try {
                    consumer.close(Duration.ofMillis(1000));
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static String buildJaas(String username, String password, String mechanism) {
        String loginModule;
        if (mechanism != null && mechanism.toUpperCase().startsWith("SCRAM-")) {
            loginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        } else {
            loginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        }
        String safeUser = escapeJaas(username);
        String safePass = escapeJaas(password);
        return loginModule + " required username=\"" + safeUser + "\" password=\"" + safePass + "\";";
    }

    private static String escapeJaas(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static CliArgs parseArgs(String[] args) {
        CliArgs cli = new CliArgs();

        cli.bootstrap = required(getArg(args, "--bootstrap"), "--bootstrap");
        cli.topic = required(getArg(args, "--topic"), "--topic");
        cli.group = required(getArg(args, "--group"), "--group");
        cli.useGroup = parseBool(required(getArg(args, "--use-group"), "--use-group"));
        cli.useAuth = parseBool(required(getArg(args, "--use-auth"), "--use-auth"));

        cli.securityProtocol = defaultIfEmpty(getArg(args, "--security-protocol"), "SASL_PLAINTEXT");
        cli.anonymousSecurityProtocol = defaultIfEmpty(getArg(args, "--anonymous-security-protocol"), "");
        cli.saslMechanism = defaultIfEmpty(getArg(args, "--sasl-mechanism"), "SCRAM-SHA-256");

        cli.socketTimeoutMs = parseInt(defaultIfEmpty(getArg(args, "--socket-timeout-ms"), "4000"), "--socket-timeout-ms");
        cli.requestTimeoutMs = parseInt(defaultIfEmpty(getArg(args, "--request-timeout-ms"), "7000"), "--request-timeout-ms");
        cli.assignmentTimeoutMs = parseInt(defaultIfEmpty(getArg(args, "--assignment-timeout-ms"), "10000"), "--assignment-timeout-ms");
        cli.consumeTimeoutMs = parseInt(defaultIfEmpty(getArg(args, "--consume-timeout-ms"), "10000"), "--consume-timeout-ms");
        cli.pollTimeoutMs = parseInt(defaultIfEmpty(getArg(args, "--poll-timeout-ms"), "500"), "--poll-timeout-ms");
        cli.sendTimeoutMs = parseInt(defaultIfEmpty(getArg(args, "--send-timeout-ms"), "8000"), "--send-timeout-ms");
        cli.clientIdPrefix = defaultIfEmpty(getArg(args, "--client-id-prefix"), "java-auth");

        if (cli.useAuth) {
            cli.username = required(getArg(args, "--username"), "--username");
            cli.password = required(getArg(args, "--password"), "--password");
        } else {
            cli.username = defaultIfEmpty(getArg(args, "--username"), "");
            cli.password = defaultIfEmpty(getArg(args, "--password"), "");
        }

        return cli;
    }

    private static String getArg(String[] args, String key) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }

    private static String required(String value, String key) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("missing required arg: " + key);
        }
        return value;
    }

    private static String defaultIfEmpty(String value, String defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }

    private static boolean parseBool(String value) {
        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    private static int parseInt(String value, String key) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid int for " + key + ": " + value);
        }
    }

    private static String uuid8() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private static String sanitize(String text) {
        String value = text == null ? "" : text;
        value = value.replace("\n", " ").replace("\r", " ").trim();
        if (value.length() > DETAIL_MAX_LEN) {
            return value.substring(0, DETAIL_MAX_LEN - 3) + "...";
        }
        return value;
    }

    private static String shortError(Throwable t) {
        if (t == null) {
            return "";
        }
        List<Throwable> chain = new ArrayList<>();
        Throwable cursor = t;
        while (cursor != null && chain.size() < 4) {
            chain.add(cursor);
            cursor = cursor.getCause();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < chain.size(); i++) {
            Throwable item = chain.get(i);
            if (i > 0) {
                sb.append(" <- ");
            }
            sb.append(item.getClass().getSimpleName());
            String msg = item.getMessage();
            if (msg != null && !msg.trim().isEmpty()) {
                sb.append(": ").append(msg.trim());
            }
        }
        return sanitize(sb.toString());
    }
}
