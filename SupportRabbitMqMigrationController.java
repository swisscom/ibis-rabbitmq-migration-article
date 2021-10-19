import com.rabbitmq.client.GetResponse;
import com.swisscom.entc.ibis.billing.processor.listener.DeadLetterQueueService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.swagger.annotations.Api;

@RestController
@RequestMapping("/support/migration")
@Api(tags = "Support", description = "Diverse Support Endpoints")
public class SupportRabbitMqMigrationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(SupportRabbitMqMigrationController.class);

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    List<Binding> rabbitBindings;

    @Autowired
    DeadLetterQueueService deadLetterQueueService;

    @GetMapping("/quorum")
    public void quorumMigrate() throws InterruptedException {
        String quorumQueue = "my-quorum-queue";
        String classicQueue = "my-classic-mirrored-queue";

        LOGGER.info("Migrating {} queue to quorum queue {}", classicQueue, quorumQueue);

        RabbitAdmin admin = new RabbitAdmin(rabbitTemplate.getConnectionFactory());

        QueueInformation classicQueueInfo = admin.getQueueInfo(classicQueue);
        if (classicQueueInfo == null) {
            LOGGER.info("Queue {} does not exist. Nothing to migrate", classicQueue);
            return;
        }

        // Remove all bindings to prevent new messages from coming
        // Since the bindings are the same for new and old queues,
        // we can reuse binding definitions to delete old ones (replacing the queue name as necessary)
        rabbitBindings.stream()
                .filter(b -> b.getDestination().equals(quorumQueue))
                .map(b -> new Binding(classicQueue, b.getDestinationType(), b.getExchange(), b.getRoutingKey(), null))
                .forEach(b -> {
                    LOGGER.info("removing binding from {} to {}, routing key {}", b.getExchange(), b.getDestination(), b.getRoutingKey());
                    admin.removeBinding(b);
                });

        LOGGER.info("Waiting few seconds until all messages are in");
        TimeUnit.SECONDS.sleep(5);

        int deadLetterMessageCount = deadLetterQueueService.getCount();

        LOGGER.info("Draining the classic queue - rejecting everything to move to dead letter queue");
        int drainableMessageCount = classicQueueInfo.getMessageCount();
        rabbitTemplate.execute(channel -> {
            int c = drainableMessageCount;

            // We use a bunch of AMQP primitives to interact with the queue
            channel.basicQos(c);
            while (c > 0) {
                GetResponse resp = channel.basicGet(classicQueue, false);
                channel.basicReject(resp.getEnvelope().getDeliveryTag(), false);
                c--;
            }

            return null;
        });

        int expectedDeadLetterMessageCount = deadLetterMessageCount + drainableMessageCount;

        LOGGER.info("Waiting for the dead letter queue to receive all rejected messages");
        Instant timeout = Instant.now().plusSeconds(10);
        while (deadLetterQueueService.getCount() < expectedDeadLetterMessageCount) {
            TimeUnit.SECONDS.sleep(1);
            if (Instant.now().isAfter(timeout)) {
                throw new RuntimeException("Migration failed - dead letter queue has not received all expected messages within 10 seconds");
            }
        }

        // Consume all messages from dead letter queue
        LOGGER.info("Consuming all moved messages from dead letter queue");
        deadLetterQueueService.processQueue(drainableMessageCount);

        LOGGER.info("Deleting the queue only if unused and not empty");
        admin.deleteQueue(classicQueue, true, true);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            quorumMigrate();
        } catch (Exception e) {
            LOGGER.error("Error while executing automated quorum queue migration.", e);
        }
    }
}
