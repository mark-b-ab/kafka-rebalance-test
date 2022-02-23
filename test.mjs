import { registerGracefulShutdown } from './gracefull-stop.mjs';
import { setTimeout }               from 'timers/promises';
import { EventEmitter }             from 'events';
import { kafka }                    from './kafka.mjs';
import lodash                       from 'lodash';

const processedMessages = [];

async function process({ partition, message: { value, offset } }) {
    console.log(`⏳  Partition -> ${partition}  Offset -> ${offset} Message: ${value.toString()}`);

    processedMessages.push(value.toString());

    await setTimeout(500);

    console.log(`✅  Partition -> ${partition}  Offset -> ${offset} Message: ${value.toString()}`);
}

async function runConsumers() {
    const consumer1 = kafka.consumer({ groupId: 'test-group' });
    const consumer2 = kafka.consumer({ groupId: 'test-group' });

    console.log('Test: connecting');

    await consumer1.connect();
    await consumer2.connect();

    console.log('Test: connected, subscribing');

    await consumer1.subscribe({ topic: 'bench-topic', fromBeginning: true });
    await consumer2.subscribe({ topic: 'bench-topic', fromBeginning: true });

    const emitter = new EventEmitter();

    emitter.once('start', () => {
        consumer2.run({
            autoCommitInterval: -1,
            eachMessage: async (message) => {
                await process(message);
                setImmediate(() => emitter.emit('stop'))
            },
        });
    });

    emitter.once('stop', () => consumer2.stop());

    await consumer1.run({
        autoCommitInterval: -1,
        eachMessage: async (message) => {
            emitter.emit('start');
            await process(message);
        },
    });

    registerGracefulShutdown([consumer1, consumer2], () => {
        console.log(`Processed jobs: ${processedMessages}`);

        const duplicates = lodash.filter(processedMessages, (val, i, iteratee) => lodash.includes(iteratee, val, i + 1));

        if (duplicates.length > 0) {
            console.error(`Result:\x1b[31m Duplicates found: ${duplicates} \x1b[0m`);
        } else {
            console.log(`Result:\x1b[32m No duplicates found \x1b[0m`);
        }
    });
}

await runConsumers();