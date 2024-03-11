import { Channel, ConsumeMessage } from 'amqplib';
import * as chai from 'chai';
import chaiJest from 'chai-jest';
import { once } from 'events';
import { defer, timeout } from 'promise-tools';
import amqp, { AmqpConnectionManagerClass as AmqpConnectionManager } from '../src';
import { IAmqpConnectionManager } from '../src/AmqpConnectionManager';
jest.setTimeout(3e6);

chai.use(chaiJest);

const { expect } = chai;

/**
 * Tests in this file assume you have a RabbitMQ instance running on localhost.
 * You can start one with:
 *
 *   docker-compose up -d
 *
 */
describe('Integration tests', () => {
    let connection: IAmqpConnectionManager;

    afterEach(async () => {
        await connection?.close();
    });

    it('should connect to the broker', async () => {
        // Create a new connection manager
        connection = amqp.connect(['amqp://localhost']);
        await timeout(once(connection, 'connect'), 3000);
    });

    it('should connect to the broker with a username and password', async () => {
        // Create a new connection manager
        connection = amqp.connect(['amqp://guest:guest@localhost:5672']);
        await timeout(once(connection, 'connect'), 3000);
    });

    it('should connect to the broker with a string', async () => {
        // Create a new connection manager
        connection = amqp.connect('amqp://guest:guest@localhost:5672');
        await timeout(once(connection, 'connect'), 3000);
    });

    it('should connect to the broker with a amqp.Connect object', async () => {
        // Create a new connection manager
        connection = amqp.connect({
            protocol: 'amqp',
            hostname: 'localhost',
            port: 5672,
            vhost: '/',
        });
        await timeout(once(connection, 'connect'), 3000);
    });

    it('should connect to the broker with an url/options object', async () => {
        // Create a new connection manager
        connection = amqp.connect({
            url: 'amqp://guest:guest@localhost:5672',
        });
        await timeout(once(connection, 'connect'), 3000);
    });

    it('should connect to the broker with a string with options', async () => {
        // Create a new connection manager
        connection = amqp.connect(
            'amqp://guest:guest@localhost:5672/%2F?heartbeat=10&channelMax=100'
        );
        await timeout(once(connection, 'connect'), 3000);
    });

    // This test might cause jest to complain about leaked resources due to the bug described and fixed by:
    // https://github.com/squaremo/amqp.node/pull/584
    it('should throw on awaited connect with wrong password', async () => {
        connection = new AmqpConnectionManager('amqp://guest:wrong@localhost');
        let err;
        try {
            await connection.connect();
        } catch (error: any) {
            err = error;
        }
        expect(err.message).to.contain('ACCESS-REFUSED');
    });

    it('send and receive messages', async () => {
        const exchangeName = 'testExchange1';
        const queueName = 'testQueue1';
        const content = `hello world - ${Date.now()}`;

        // Create a new connection manager
        connection = amqp.connect(['amqp://localhost']);

        // Ask the connection manager for a ChannelWrapper.  Specify a setup function to
        // run every time we reconnect to the broker.
        const sendChannel = connection.createChannel({
            setup: async (channel: Channel) => {
                await channel.assertExchange(exchangeName, 'topic', {
                    autoDelete: true,
                    durable: false,
                });
                await channel.assertQueue(queueName, { durable: false, autoDelete: true });
                await channel.bindQueue(queueName, exchangeName, '');
            },
        });

        const rxPromise = defer<ConsumeMessage>();

        const receiveWrapper = connection.createChannel({
            setup: async (channel: Channel) => {
                // `channel` here is a regular amqplib `Channel`.
                // Note that `this` here is the channelWrapper instance.
                await channel.assertQueue(queueName, { durable: false, autoDelete: true });
                await channel.consume(
                    queueName,
                    (message) => {
                        if (!message) {
                            // Ignore.
                        } else if (message.content.toString() === content) {
                            rxPromise.resolve(message);
                        } else {
                            console.log(
                                `Received message ${message?.content.toString()} !== ${content}`
                            );
                        }
                    },
                    {
                        noAck: true,
                    }
                );
            },
        });

        await timeout(once(connection, 'connect'), 3000);

        await sendChannel.publish(exchangeName, '', Buffer.from(content));

        const result = await timeout(rxPromise.promise, 3000);
        expect(result.content.toString()).to.equal(content);

        await sendChannel.close();
        await receiveWrapper.close();
    });

    it('send and receive messages with plain channel', async () => {
        const exchangeName = 'testEx2';
        const queueName = 'testQueue2';
        const content = `hello world - ${Date.now()}`;

        connection = new AmqpConnectionManager('amqp://localhost');
        const sendChannel = connection.createChannel({
            setup: async (channel: Channel) => {
                await channel.assertExchange(exchangeName, 'topic', { durable: false });
                await channel.assertQueue(queueName, { durable: false, autoDelete: true });
                await channel.bindQueue(queueName, exchangeName, '');
            },
        });

        const receiveChannel = connection.createChannel({
            setup: async (channel: Channel) => {
                await channel.assertQueue(queueName, { durable: false, autoDelete: true });
            },
        });

        await connection.connect();

        const rxPromise = defer<ConsumeMessage>();
        await receiveChannel.consume(queueName, (message) => {
            rxPromise.resolve(message);
        });

        await sendChannel.publish(exchangeName, '', Buffer.from(content));

        const result = await timeout(rxPromise.promise, 3000);
        expect(result.content.toString()).to.equal(content);

        await sendChannel.close();
        await receiveChannel.close();
    });
});
