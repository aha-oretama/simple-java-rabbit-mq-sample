package jp.aha.oretama.simple.java.rabbitmq.sample;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Yasufumi Sekine on 2018/04/06.
 */
public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String severity = getSeverity(args);
        String message = getMessage(args);

        channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
        System.out.println(" [x] Sent '" + severity + "':'" + message + "'");

        channel.close();
        connection.close();
    }

    private static String getMessage(String[] strings) {
        if(strings.length < 2) {
            return "Hello World!";
        }

        return Stream.of(strings).skip(1).collect(Collectors.joining(" "));
    }

    private static String getSeverity(String[] strings) {
        if(strings.length < 1) {
            return "info";
        }
        return strings[0];
    }
}
