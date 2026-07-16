const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'user-server',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'user-consumer-server1',
  maxInFlightRequests: 10,
});

const producer = kafka.producer({
  maxInFlightRequests: 10,
});

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
  await producer.connect();
  await consumer.connect();

  consumer.on(consumer.events.GROUP_JOIN, e => {
    console.dir(e.payload.memberAssignment, { depth: null });
  });

  await consumer.subscribe({
    topic: 'scada_value.get',
    fromBeginning: false,
  });

  await consumer.subscribe({
    topic: 'scada_value.list',
    fromBeginning: false,
  });

  console.log('consumer started');

  await consumer.run({
    partitionsConsumedConcurrently: 10,
    autoCommitThreshold: 100,
    autoCommitInterval: 1000,
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const correlationId = message.headers['kafka_correlationId']?.toString();
      const replyTopic = message.headers['kafka_replyTopic']?.toString();

      console.log('START', topic, partition, new Date().toISOString());

      await sleep(3000);

      let response;
      try {
        response = { err: null, response: await handle(topic, message) };
      } catch (e) {
        response = { err: { message: e.message }, response: null };
      }

      if (replyTopic && correlationId) {
        await producer.send({
          topic: replyTopic,
          messages: [{
            key: message.key,
            value: JSON.stringify(response),
            headers: {
              kafka_correlationId: correlationId,
            },
          }],
        });
      }
    },
  });
}

main().catch(console.error);

async function handle(topic, message) {
  const payload = JSON.parse(message.value.toString());
  switch (topic) {
    case 'scada_value.get':
      return {valueId: payload.id};
    default:
      throw new Error(`Unknown topic: ${topic}`);
  }
}
