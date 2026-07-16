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
    eachMessage: async ({
                          topic,
                          partition,
                          message,
                        }) => {

      console.log('START', topic, partition, new Date().toISOString());
      await sleep(3000);
    },
  });
}

main().catch(console.error);
