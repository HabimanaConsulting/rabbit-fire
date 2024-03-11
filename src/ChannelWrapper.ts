import type * as amqplib from 'amqplib';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';
import { promisify } from 'util';
import { IAmqpConnectionManager } from './AmqpConnectionManager.js';

const MAX_MESSAGES_PER_BATCH = 1000;

const randomBytes = promisify(crypto.randomBytes);

export type Channel = amqplib.Channel;

export type SetupFunc = (channel: Channel) => Promise<void>;

export interface CreateChannelOpts {
    /**  Name for this channel. Used for debugging. */
    name?: string;
    /**
     * A function to call whenever we reconnect to the broker (and therefore create a new underlying channel.)
     * This function should return a Promise.
     */
    setup?: SetupFunc;
}

export type PublishOptions = amqplib.Options.Publish;

interface PublishMessage {
    exchange: string;
    routingKey: string;
    content: Buffer;
    options?: PublishOptions;
    resolve: (result: boolean) => void;
    reject: (err: Error) => void;
    isTimedout: boolean;
}

export interface ConsumerOptions extends amqplib.Options.Consume {
    prefetch?: number;
}

export interface Consumer {
    consumerTag: string | null;
    queue: string;
    onMessage: (msg: amqplib.ConsumeMessage) => void;
    options: ConsumerOptions;
}

type Message = PublishMessage;

// const IRRECOVERABLE_ERRORS = [
//     403, // AMQP Access Refused Error.
//     404, // AMQP Not Found Error.
//     406, // AMQP Precondition Failed Error.
//     501, // AMQP Frame Error.
//     502, // AMQP Frame Syntax Error.
//     503, // AMQP Invalid Command Error.
//     504, // AMQP Channel Not Open Error.
//     505, // AMQP Unexpected Frame.
//     530, // AMQP Not Allowed Error.
//     540, // AMQP Not Implemented Error.
//     541, // AMQP Internal Error.
// ];

/**
 * Calls to `publish()` work just like in amqplib, but messages are queued internally and
 * are guaranteed to be delivered.  If the underlying connection drops, ChannelWrapper will wait for a new
 * connection and continue.
 *
 * Events:
 * * `connect` - emitted every time this channel connects or reconnects.
 * * `error(err, {name})` - emitted if an error occurs setting up the channel.
 * * `drop({message, err})` - called when a JSON message was dropped because it could not be encoded.
 * * `close` - emitted when this channel closes via a call to `close()`
 *
 */
export default class ChannelWrapper extends EventEmitter {
    private _connectionManager: IAmqpConnectionManager;

    /** If we're in the process of creating a channel, this is a Promise which
     * will resolve when the channel is set up.  Otherwise, this is `null`.
     */
    private _settingUp: Promise<void> | undefined = undefined;
    private _setups: SetupFunc[];
    /** Queued messages, not yet sent. */
    private _messages: Message[] = [];
    /** Reason code during publish or sendtoqueue messages. */
    // private _irrecoverableCode: number | undefined;
    /** Consumers which will be reconnected on channel errors etc. */
    private _consumers: Consumer[] = [];

    /**
     * The currently connected channel.  Note that not all setup functions
     * have been run on this channel until `@_settingUp` is either null or
     * resolved.
     */
    private _channel?: Channel;

    /**
     * True if the "worker" is busy sending messages.  False if we need to
     * start the worker to get stuff done.
     */
    private _working = false;

    /**
     *  We kill off workers when we disconnect.  Whenever we start a new
     * worker, we bump up the `_workerNumber` - this makes it so if stale
     * workers ever do wake up, they'll know to stop working.
     */
    private _workerNumber = 0;

    /**
     * True if the underlying channel has room for more messages.
     */
    private _channelHasRoom = true;

    public name?: string;

    addListener(event: string, listener: (...args: any[]) => void): this;
    addListener(event: 'connect', listener: () => void): this;
    addListener(event: 'error', listener: (err: Error, info: { name: string }) => void): this;
    addListener(event: 'close', listener: () => void): this;
    addListener(event: string, listener: (...args: any[]) => void): this {
        return super.addListener(event, listener);
    }

    on(event: string, listener: (...args: any[]) => void): this;
    on(event: 'connect', listener: () => void): this;
    on(event: 'error', listener: (err: Error, info: { name: string }) => void): this;
    on(event: 'close', listener: () => void): this;
    on(event: string, listener: (...args: any[]) => void): this {
        return super.on(event, listener);
    }

    once(event: string, listener: (...args: any[]) => void): this;
    once(event: 'connect', listener: () => void): this;
    once(event: 'error', listener: (err: Error, info: { name: string }) => void): this;
    once(event: 'close', listener: () => void): this;
    once(event: string, listener: (...args: any[]) => void): this {
        return super.once(event, listener);
    }

    prependListener(event: string, listener: (...args: any[]) => void): this;
    prependListener(event: 'connect', listener: () => void): this;
    prependListener(event: 'error', listener: (err: Error, info: { name: string }) => void): this;
    prependListener(event: 'close', listener: () => void): this;
    prependListener(event: string, listener: (...args: any[]) => void): this {
        return super.prependListener(event, listener);
    }

    prependOnceListener(event: string, listener: (...args: any[]) => void): this;
    prependOnceListener(event: 'connect', listener: () => void): this;
    prependOnceListener(
        event: 'error',
        listener: (err: Error, info: { name: string }) => void
    ): this;
    prependOnceListener(event: 'close', listener: () => void): this;
    prependOnceListener(event: string, listener: (...args: any[]) => void): this {
        return super.prependOnceListener(event, listener);
    }

    /**
     * Returns a Promise which resolves when this channel next connects.
     * (Mainly here for unit testing...)
     *
     * @returns - Resolves when connected.
     */
    waitForConnect(): Promise<void> {
        return this._channel && !this._settingUp
            ? Promise.resolve()
            : new Promise((resolve) => this.once('connect', resolve));
    }

    /*
     * Publish a message to the channel.
     *
     * This works just like amqplib's `publish()`, except if the channel is not
     * connected, this will wait until the channel is connected.  Returns a
     * Promise which will only resolve when the message has been succesfully sent.
     * The returned promise will be rejected if `close()` is called on this
     * channel before it can be sent, if `options.json` is set and the message
     * can't be encoded, or if the broker rejects the message for some reason.
     *
     */
    publish(
        exchange: string,
        routingKey: string,
        content: Buffer,
        options?: PublishOptions
    ): Promise<boolean> {
        return new Promise<boolean>((resolve, reject) => {
            const opts = options || {};
            if (!this._settingUp && !!this._channel && this._channelHasRoom) {
                this._channelHasRoom = this._channel.publish(
                    exchange,
                    routingKey,
                    content,
                    options
                );
                resolve(this._channelHasRoom);
            } else {
                this._enqueueMessage({
                    exchange,
                    routingKey,
                    content,
                    resolve,
                    reject,
                    options: opts,
                    isTimedout: false,
                });
            }
        });
    }

    private _enqueueMessage(message: Message) {
        this._messages.push(message);
    }

    /**
     * Create a new ChannelWrapper.
     *
     * @param connectionManager - connection manager which
     *   created this channel.
     * @param [options] -
     * @param [options.name] - A name for this channel.  Handy for debugging.
     * @param [options.setup] - A default setup function to call.  See
     *   `addSetup` for details.
     * @param [options.json] - if true, then ChannelWrapper assumes all
     *   messages passed to `publish()` and `sendToQueue()` are plain JSON objects.
     *   These will be encoded automatically before being sent.
     *
     */
    constructor(connectionManager: IAmqpConnectionManager, options: CreateChannelOpts = {}) {
        super();
        this._onConnect = this._onConnect.bind(this);
        this._onDisconnect = this._onDisconnect.bind(this);
        this._connectionManager = connectionManager;
        this.name = options.name;

        // Array of setup functions to call.
        this._setups = [];
        this._consumers = [];

        if (options.setup) {
            this._setups.push(options.setup);
        }

        const connection = connectionManager.connection;
        if (connection) {
            this._onConnect({ connection });
        }
        connectionManager.on('connect', this._onConnect);
        connectionManager.on('disconnect', this._onDisconnect);
    }

    // Called whenever we connect to the broker.
    private async _onConnect({ connection }: { connection: amqplib.Connection }): Promise<void> {
        // this._irrecoverableCode = undefined;

        try {
            const channel: Channel = await connection.createChannel();

            this._channel = channel;
            this._channelHasRoom = true;
            channel.on('close', () => this._onChannelClose(channel));
            channel.on('drain', () => this._onChannelDrain());

            this._settingUp = Promise.all(
                this._setups.map(async (setupFn) =>
                    setupFn(channel).catch((err) => {
                        if (err.name === 'IllegalOperationError') {
                            // Don't emit an error if setups failed because the channel closed.
                            return;
                        }
                        this.emit('error', err, { name: this.name });
                    })
                )
            )
                .then(() => {
                    return Promise.all(this._consumers.map((c) => this._reconnectConsumer(c)));
                })
                .then(() => {
                    this._settingUp = undefined;
                });
            await this._settingUp;

            if (!this._channel) {
                // Can happen if channel closes while we're setting up.
                return;
            }

            // Since we just connected, publish any queued messages
            this._startWorker();
            this.emit('connect');
        } catch (err) {
            this.emit('error', err, { name: this.name });
            this._settingUp = undefined;
            this._channel = undefined;
        }
    }

    /**
     * Called whenever the channel closes.
     *
     * @param channel amqplib.Channel
     */
    private _onChannelClose(channel: Channel): void {
        if (this._channel === channel) {
            this._channel = undefined;
        }
        // Wait for another reconnect to create a new channel.
    }

    /**
     * Called whenever the channel drains.
     */
    private _onChannelDrain(): void {
        this._channelHasRoom = true;
        this._startWorker();
    }

    // Called whenever we disconnect from the AMQP server.
    private _onDisconnect(_: { err: Error & { code: number } }): void {
        // this._irrecoverableCode = ex.err instanceof Error ? ex.err.code : undefined;
        this._channel = undefined;
        this._settingUp = undefined;

        // Kill off the current worker.  We never get any kind of error for messages in flight - see
        // https://github.com/squaremo/amqp.node/issues/191.
        this._working = false;
    }

    // Returns the number of unsent messages queued on this channel.
    queueLength(): number {
        return this._messages.length;
    }

    /**
     * Destroy this channel.
     *
     * Any unsent messages will have their associated Promises rejected.
     * @returns {void}
     */
    async close(): Promise<void> {
        this._working = false;
        if (this._messages.length !== 0) {
            // Reject any unsent messages.
            this._messages.forEach((message) => {
                message.reject(new Error('Channel closed'));
            });
        }

        this._connectionManager.removeListener('connect', this._onConnect);
        this._connectionManager.removeListener('disconnect', this._onDisconnect);
        const answer = (this._channel && this._channel.close()) || undefined;
        this._channel = undefined;

        this.emit('close');

        return answer;
    }

    private _shouldPublish(): boolean {
        return (
            this._messages.length > 0 && !this._settingUp && !!this._channel && this._channelHasRoom
        );
    }

    /**
     * Start publishing queued messages, if there isn't already a worker doing this.
     */
    private _startWorker(): void {
        if (!this._working && this._shouldPublish()) {
            this._working = true;
            this._workerNumber++;
            this._publishExchangeMessages(this._workerNumber);
        }
    }

    /**
     * Send messages in batches of 1000 - don't want to starve the event loop.
     *
     * @param workerNumber
     * @returns {void}
     */
    private _publishExchangeMessages(workerNumber: number): void {
        const channel = this._channel;
        if (
            !channel ||
            !this._shouldPublish() ||
            !this._working ||
            workerNumber !== this._workerNumber
        ) {
            // Can't publish anything right now...
            this._working = false;
            return;
        }

        try {
            // Send messages in batches of 1000 - don't want to starve the event loop.
            let sendsLeft = MAX_MESSAGES_PER_BATCH;
            while (this._channelHasRoom && this._messages.length > 0 && sendsLeft > 0) {
                sendsLeft--;

                const message = this._messages.shift();
                if (!message) {
                    break;
                }

                let thisCanSend = true;
                thisCanSend = this._channelHasRoom = channel.publish(
                    message.exchange,
                    message.routingKey,
                    message.content,
                    message.options
                );
                message.resolve(thisCanSend);
            }

            // If we didn't send all the messages, send some more...
            if (this._channelHasRoom && this._messages.length > 0) {
                setImmediate(() => this._publishExchangeMessages(workerNumber));
            } else {
                this._working = false;
            }

            /* istanbul ignore next */
        } catch (err) {
            this._working = false;
            this.emit('error', err);
        }
    }

    /**
     * Setup a consumer
     * This consumer will be reconnected on cancellation and channel errors.
     */
    async consume(
        queue: string,
        onMessage: Consumer['onMessage'],
        options: ConsumerOptions = {}
    ): Promise<amqplib.Replies.Consume> {
        const consumerTag = options.consumerTag || (await randomBytes(16)).toString('hex');
        const consumer: Consumer = {
            consumerTag: null,
            queue,
            onMessage,
            options: {
                ...options,
                consumerTag,
            },
        };

        if (this._settingUp) {
            await this._settingUp;
        }

        this._consumers.push(consumer);
        await this._consume(consumer);
        return { consumerTag };
    }

    private async _consume(consumer: Consumer): Promise<void> {
        if (!this._channel) {
            return;
        }

        const { prefetch, ...options } = consumer.options;
        if (typeof prefetch === 'number') {
            this._channel.prefetch(prefetch, false);
        }

        const { consumerTag } = await this._channel.consume(
            consumer.queue,
            (msg) => {
                if (!msg) {
                    consumer.consumerTag = null;
                    this._reconnectConsumer(consumer).catch((err) => {
                        if (err.code === 404) {
                            // Ignore errors caused by queue not declared. In
                            // those cases the connection will reconnect and
                            // then consumers reestablished. The full reconnect
                            // might be avoided if we assert the queue again
                            // before starting to consume.
                            return;
                        }
                        this.emit('error', err);
                    });
                    return;
                }
                consumer.onMessage(msg);
            },
            options
        );
        consumer.consumerTag = consumerTag;
    }

    private async _reconnectConsumer(consumer: Consumer): Promise<void> {
        if (!this._consumers.includes(consumer)) {
            // Intentionally canceled
            return;
        }
        await this._consume(consumer);
    }

    /**
     * Cancel all consumers
     */
    async cancelAll(): Promise<void> {
        const consumers = this._consumers;
        this._consumers = [];
        if (!this._channel) {
            return;
        }

        const channel = this._channel;
        await Promise.all(
            consumers.reduce<any[]>((acc, consumer) => {
                if (consumer.consumerTag) {
                    acc.push(channel.cancel(consumer.consumerTag));
                }
                return acc;
            }, [])
        );
    }

    async cancel(consumerTag: string): Promise<void> {
        const idx = this._consumers.findIndex((x) => x.options.consumerTag === consumerTag);
        if (idx === -1) {
            return;
        }

        const consumer = this._consumers[idx];
        this._consumers.splice(idx, 1);
        if (this._channel && consumer.consumerTag) {
            await this._channel.cancel(consumer.consumerTag);
        }
    }

    /**
     * Send an `ack` to the underlying channel.
     *
     * @param message amqplib.Message
     * @param allUpTo boolean
     */
    ack(message: amqplib.Message, allUpTo?: boolean): void {
        this._channel && this._channel.ack(message, allUpTo);
    }

    /** Send an `ackAll` to the underlying channel. */
    ackAll(): void {
        this._channel && this._channel.ackAll();
    }

    /** Send a `nack` to the underlying channel. */
    nack(message: amqplib.Message, allUpTo?: boolean, requeue?: boolean): void {
        this._channel && this._channel.nack(message, allUpTo, requeue);
    }

    /** Send a `nackAll` to the underlying channel. */
    nackAll(requeue?: boolean): void {
        this._channel && this._channel.nackAll(requeue);
    }

    /** Send a `purgeQueue` to the underlying channel. */
    async purgeQueue(queue: string): Promise<amqplib.Replies.PurgeQueue> {
        if (this._channel) {
            return await this._channel.purgeQueue(queue);
        } else {
            throw new Error(`Not connected.`);
        }
    }

    /** Send a `checkQueue` to the underlying channel. */
    async checkQueue(queue: string): Promise<amqplib.Replies.AssertQueue> {
        if (this._channel) {
            return await this._channel.checkQueue(queue);
        } else {
            throw new Error(`Not connected.`);
        }
    }

    /** Send a `assertQueue` to the underlying channel. */
    async assertQueue(
        queue: string,
        options?: amqplib.Options.AssertQueue
    ): Promise<amqplib.Replies.AssertQueue> {
        if (this._channel) {
            return await this._channel.assertQueue(queue, options);
        } else {
            return { queue, messageCount: 0, consumerCount: 0 };
        }
    }

    /** Send a `bindQueue` to the underlying channel. */
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async bindQueue(queue: string, source: string, pattern: string, args?: any): Promise<void> {
        if (this._channel) {
            await this._channel.bindQueue(queue, source, pattern, args);
        }
    }

    /** Send a `unbindQueue` to the underlying channel. */
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async unbindQueue(queue: string, source: string, pattern: string, args?: any): Promise<void> {
        if (this._channel) {
            await this._channel.unbindQueue(queue, source, pattern, args);
        }
    }

    /** Send a `deleteQueue` to the underlying channel. */
    async deleteQueue(
        queue: string,
        options?: amqplib.Options.DeleteQueue
    ): Promise<amqplib.Replies.DeleteQueue> {
        if (this._channel) {
            return await this._channel.deleteQueue(queue, options);
        } else {
            throw new Error(`Not connected.`);
        }
    }

    /** Send a `assertExchange` to the underlying channel. */
    async assertExchange(
        exchange: string,
        type: 'direct' | 'topic' | 'headers' | 'fanout' | 'match' | string,
        options?: amqplib.Options.AssertExchange
    ): Promise<amqplib.Replies.AssertExchange> {
        if (this._channel) {
            return await this._channel.assertExchange(exchange, type, options);
        } else {
            return { exchange };
        }
    }

    /** Send a `bindExchange` to the underlying channel. */
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async bindExchange(
        destination: string,
        source: string,
        pattern: string,
        args?: any
    ): Promise<amqplib.Replies.Empty> {
        if (this._channel) {
            return await this._channel.bindExchange(destination, source, pattern, args);
        } else {
            throw new Error(`Not connected.`);
        }
    }

    /** Send a `checkExchange` to the underlying channel. */
    async checkExchange(exchange: string): Promise<amqplib.Replies.Empty> {
        if (this._channel) {
            return await this._channel.checkExchange(exchange);
        } else {
            throw new Error(`Not connected.`);
        }
    }

    /** Send a `deleteExchange` to the underlying channel. */
    async deleteExchange(
        exchange: string,
        options?: amqplib.Options.DeleteExchange
    ): Promise<amqplib.Replies.Empty> {
        if (this._channel) {
            return await this._channel.deleteExchange(exchange, options);
        } else {
            throw new Error(`Not connected.`);
        }
    }

    /** Send a `unbindExchange` to the underlying channel. */
    // eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
    async unbindExchange(
        destination: string,
        source: string,
        pattern: string,
        args?: any
    ): Promise<amqplib.Replies.Empty> {
        if (this._channel) {
            return await this._channel.unbindExchange(destination, source, pattern, args);
        } else {
            throw new Error(`Not connected.`);
        }
    }

    /** Send a `get` to the underlying channel. */
    async get(queue: string, options?: amqplib.Options.Get): Promise<amqplib.GetMessage | false> {
        if (this._channel) {
            return await this._channel.get(queue, options);
        } else {
            throw new Error(`Not connected.`);
        }
    }
}
