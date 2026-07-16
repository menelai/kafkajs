const { Kafka, logLevel } = require('kafkajs');
const { randomUUID } = require('crypto');

async function test() {
  const kafka = new Kafka({
    clientId: 'test-producer',
    brokers: ['localhost:9092'],
    logLevel: logLevel.INFO,
  });

  const producer = kafka.producer({});
  const consumer = kafka.consumer({
    // groupId должен быть уникальным на инстанс клиента,
    // иначе несколько запущенных клиентов будут делить partitions
    // reply-топика между собой и терять свои же ответы
    groupId: `test-client-reply-${randomUUID()}`,
  });

  const REPLY_TOPIC = 'scada_value.get.reply'; // топик, который слушает клиент

  const pending = new Map(); // correlationId -> { resolve, reject }

  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: REPLY_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const correlationId = message.headers?.kafka_correlationId?.toString();
      if (!correlationId) return;

      const waiter = pending.get(correlationId);
      if (!waiter) return; // ответ не наш / уже протух

      pending.delete(correlationId);
      const payload = JSON.parse(message.value.toString());
      waiter.resolve(payload);
    },
  });

  function sendRequest(topic, value, timeoutMs = 10000) {
    const correlationId = randomUUID();

    const promise = new Promise((resolve, reject) => {
      pending.set(correlationId, { resolve, reject });
      setTimeout(() => {
        if (pending.has(correlationId)) {
          pending.delete(correlationId);
          reject(new Error(`Timeout waiting reply for ${correlationId}`));
        }
      }, timeoutMs);
    });

    producer.send({
      topic,
      messages: [
        {
          key: correlationId, // если сервер матчит по key
          value: JSON.stringify(value),
          headers: {
            kafka_correlationId: correlationId,
            kafka_replyTopic: REPLY_TOPIC,
          },
        },
      ],
    });

    return promise;
  }

  const results = await Promise.all([
    sendRequest('scada_value.get', { id: '1' }).then(v => console.log(v, new Date)),
    sendRequest('scada_value.get', { id: '2' }).then(v => console.log(v, new Date)),
    sendRequest('scada_value.get', { id: '3' }).then(v => console.log(v, new Date)),
    sendRequest('scada_value.get', { id: '4' }).then(v => console.log(v, new Date)),
    sendRequest('scada_value.get', { id: '5' }).then(v => console.log(v, new Date)),
  ]);

  await consumer.disconnect();
  await producer.disconnect();
}

test().catch(console.error);
