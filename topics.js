const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'topic-admin',
  brokers: ['localhost:9092'],
});

const admin = kafka.admin();

const topics = [
  {
    topic: 'scada_value.list',
    numPartitions: 10,
    replicationFactor: 1,
  },
  {
    topic: 'scada_value.list.reply',
    numPartitions: 1,
    replicationFactor: 1,
  },
  {
    topic: 'scada_value.get',
    numPartitions: 10,
    replicationFactor: 1,
  },
  {
    topic: 'scada_value.get.reply',
    numPartitions: 1,
    replicationFactor: 1,
  },
];

async function createTopics() {
  await admin.connect();

  try {
    const existingTopics = await admin.listTopics();
    const topicsToCreate = topics.filter(
      (t) => !existingTopics.includes(t.topic)
    );

    if (topicsToCreate.length === 0) {
      console.log('Все топики уже существуют');
      return;
    }

    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: topicsToCreate,
    });

    console.log('Результат создания:', created);
    console.log(
      'Созданы:',
      topicsToCreate.map((t) => t.topic)
    );
  } catch (err) {
    console.error('Ошибка при создании топиков:', err);
    throw err;
  } finally {
    await admin.disconnect();
  }
}

createTopics()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
