import { setTimeout } from 'timers/promises';
import lodash         from 'lodash';

const { range } = lodash;

export class ResetKafka {
    messages = 20;
    partitions = 2;
    topic = 'test-topic';

    constructor(kafka) {
        this.kafka = kafka;
    }

    async reset() {
        await this.connect();
        await this.recreateTopic();
        await this.fillTopic();
        await this.disconnect();
    }

    async connect() {
        this.admin = this.kafka.admin();
        this.producer = this.kafka.producer({});
        await this.admin.connect();
        await this.producer.connect();

        console.log('Reset: Connected to Kafka');
    }

    async disconnect() {
        await this.producer.disconnect();
        await this.admin.disconnect();

        console.log('Reset: Disconnected from Kafka');
    }

    async recreateTopic() {
        const topics = await this.admin.listTopics();

        if (topics.includes(this.topic)) {
            await this.admin.deleteTopics({
                topics: [this.topic],
            });

            console.log('Reset: Topic deleted');

            await setTimeout(1000);
        }

        await this.admin.createTopics({
            topics: [{ topic: this.topic, numPartitions: this.partitions }],
        });

        console.log('Reset: Topic created');
    }

    async fillTopic() {
        const nextPartition = this.createMessagePartitioner(this.partitions);

        await this.producer.send({
            topic: this.topic,
            messages: range(1, this.messages + 1).map((n) => ({
                partition: nextPartition(),
                key: n.toString(),
                value: n.toString(),
            })),
        });

        console.log('Reset: Topic filled');
    }

    createMessagePartitioner(count) {
        let current = count;
        return () => {
            if (current === count) {
                current = 0;
            }

            return current++;
        };
    }
}