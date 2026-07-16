const { Kafka, logLevel  } = require('kafkajs');

async function test() {
  const kafka = new Kafka({
    clientId: 'test-producer',
    brokers: ['localhost:9092'],
    logLevel: logLevel.INFO,
  });

  const producer = kafka.producer({

  });


  const consumer = kafka.consumer({
    groupId: 'test-client-reply',
  });

  await producer.connect();


  await Promise.all([
    producer
      .send({
        topic: 'scada_value.get',
        messages: [
          {
            value: JSON.stringify({ id: '2a017b76-9a16-4f7f-9724-e538a11f95d1' }),
          },
        ],
      })
      .then(console.log),
    producer
      .send({
        topic: 'scada_value.list',
        messages: [
          {
            value: JSON.stringify({ ids: [] }),
          },
        ],
      })
      .then(console.log),
  ]);

  await producer.disconnect();
}

test().catch(console.error);
