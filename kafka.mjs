import { Kafka, logLevel } from 'kafkajs';

const consoleLogger = {
    [logLevel.ERROR]: console.error,
    [logLevel.NOTHING]: console.error,
    [logLevel.WARN]: console.warn,
    [logLevel.INFO]: console.log,
    [logLevel.DEBUG]: console.debug,
};

export const kafka = new Kafka({
    brokers: ['100.100.53.77:9092'],
    clientId: 'test-client1',
    // logLevel: logLevel.DEBUG,
    logCreator: () => ({ level, log, label, namespace }) => {
        const { message, timestamp, logger, error, stack, ...others } = log;

        consoleLogger[level](`Kafka ${namespace}: ${message}`);
    },
});
