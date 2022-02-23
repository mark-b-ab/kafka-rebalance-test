export function registerGracefulShutdown(consumers, callback) {
    const errorTypes = ['unhandledRejection', 'uncaughtException'];
    const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

    errorTypes.forEach(type => {
        process.on(type, async e => {
            try {
                console.log(`process.on ${type}`);
                console.error(e);
                await Promise.allSettled(consumers.map((consumer) => consumer.disconnect()));

                if (callback) {
                    await callback()
                }


                process.exit(0);
            } catch (_) {
                process.exit(1);
            }
        });
    });

    signalTraps.forEach(type => {
        process.once(type, async () => {
            try {
                await Promise.allSettled(consumers.map((consumer) => consumer.disconnect()));

                if (callback) {
                    await callback()
                }
            } finally {
                process.kill(process.pid, type);
            }
        });
    });
}
