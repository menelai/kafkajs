# Баг kafkajs

## 1. кафка
```shell 
docker-compose up
```

## 2. Топики

```shell
node topics
```

## 3. Сервер

```shell
node srv
```

Вывод при старте:

```
{"level":"WARN","timestamp":"2026-07-16T14:07:37.982Z","logger":"kafkajs","message":"KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option \"createPartitioner: Partitioners.LegacyPartitioner\". See the migration guide at https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. Silence this warning by setting the environment variable \"KAFKAJS_NO_PARTITIONER_WARNING=1\""}
(node:15424) TimeoutNegativeWarning: -1784210858020 is a negative number.
Timeout duration was set to 1.
(Use `node --trace-warnings ...` to show where the warning was created)
consumer started
{"level":"INFO","timestamp":"2026-07-16T14:07:38.034Z","logger":"kafkajs","message":"[Consumer] Starting","groupId":"user-consumer-server1"}
{
  'scada_value.get': [
     0,  1, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19,  2,  3,
     4,  5,  6,  7,  8,  9
  ],
  'scada_value.list': [
     0,  1, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19,  2,  3,
     4,  5,  6,  7,  8,  9
  ]
}
{"level":"INFO","timestamp":"2026-07-16T14:07:41.054Z","logger":"kafkajs","message":"[ConsumerGroup] Consumer has joined the group","groupId":"user-consumer-server1","memberId":"user-server-f1a60561-386c-4eff-bb68-e146541467a9","leaderId":"user-server-f1a60561-386c-4eff-bb68-e146541467a9","isLeader":true,"memberAssignment":{"scada_value.get":[0,1,10,11,12,13,14,15,16,17,18,19,2,3,4,5,6,7,8,9],"scada_value.list":[0,1,10,11,12,13,14,15,16,17,18,19,2,3,4,5,6,7,8,9]},"groupProtocol":"RoundRobinAssigner","duration":3019}
```

## 4. Клиент

```shell
node test
```
Вывод при старте:

```
{"level":"WARN","timestamp":"2026-07-16T14:13:58.034Z","logger":"kafkajs","message":"KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option \"createPartitioner: Partitioners.LegacyPartitioner\". See the migration guide at https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. Silence this warning by setting the environment variable \"KAFKAJS_NO_PARTITIONER_WARNING=1\""}
(node:14452) TimeoutNegativeWarning: -1784211238067 is a negative number.
Timeout duration was set to 1.
(Use `node --trace-warnings ...` to show where the warning was created)
[
  {
    topicName: 'scada_value.get',
    partition: 14,
    errorCode: 0,
    baseOffset: '0',
    logAppendTime: '-1',
    logStartOffset: '0'
  }
]
[
  {
    topicName: 'scada_value.list',
    partition: 7,
    errorCode: 0,
    baseOffset: '24',
    logAppendTime: '-1',
    logStartOffset: '0'
  }
]
```

## 5. Лог srv после отправки запросов из test:

```
START scada_value.get 10 2026-07-16T18:37:36.004Z
START scada_value.get 15 2026-07-16T18:37:39.045Z
START scada_value.get 18 2026-07-16T18:37:39.045Z
START scada_value.get 1 2026-07-16T18:37:39.046Z
START scada_value.get 19 2026-07-16T18:37:39.046Z
```

## 6. Лог test после отправки запросов:

```javascript
const results = await Promise.all([
  sendRequest('scada_value.get', { id: '1' }).then(v => console.log(v, new Date)),
  sendRequest('scada_value.get', { id: '2' }).then(v => console.log(v, new Date)),
  sendRequest('scada_value.get', { id: '3' }).then(v => console.log(v, new Date)),
  sendRequest('scada_value.get', { id: '4' }).then(v => console.log(v, new Date)),
  sendRequest('scada_value.get', { id: '5' }).then(v => console.log(v, new Date)),
]);
```

```
{ err: null, response: { valueId: '1' } } 2026-07-16T18:37:39.033Z
{ err: null, response: { valueId: '5' } } 2026-07-16T18:37:42.052Z
{ err: null, response: { valueId: '2' } } 2026-07-16T18:37:42.057Z
{ err: null, response: { valueId: '3' } } 2026-07-16T18:37:42.058Z
{ err: null, response: { valueId: '4' } } 2026-07-16T18:37:42.058Z
```

Первый запрос как будто выполняется отдельно, затем остальные 4 одновременно.

Ожидается, что сервер будет принимать сообщения одновременно, а не ждать завершения одного из них. 
