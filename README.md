# rabbit-fire

Simplified connection management for amqplib, originally forked from [amqp-connection-manager](https://github.com/jwalton/node-amqp-connection-manager/tree/master).
Made to fire-and-forget messages for logging purposes or similar use cases.

## Changes

- Removed all logics for confirm channels and confirmation, while maintaining queue logic for retry.
  - Messages are sent at publish and only queued if conditions means it cannot be sent.
  - Handling of queued messages now happens at reconnect.
- Remove callbacks (and `promise-breaker`) in favor of promises only.
- Remove parsing and `json` options, all clients must pass a `Buffer`.
- Removed `sendToQueue` logic, only used for publushing to an exchange.
- Removed `addSetup`: setup on initiailization is prefered and should be enforced.
- Removed `fetchServer`: server data is known at startup.
- Build to `commonJS` (target `es2017`) uniformaly, no split between CJS and ESM.

## Features

- Automatically reconnect when your [amqplib](http://www.squaremobius.net/amqp.node/) broker dies in a fire.
- Round-robin connections between multiple brokers in a cluster.
- If messages are sent while the broker is unavailable, queues messages in memory until we reconnect.
- Supports only promisses
- Very un-opinionated library - a thin wrapper around [amqplib](http://www.squaremobius.net/amqp.node/).

## Installation

```sh
npm install --save amqplib @hcon/rabbit-fire
```

## Basics

The basic idea here is that, usually, when you create a new channel, you do some
setup work at the beginning (like asserting that various queues or exchanges
exist, or binding to queues), and then you send and receive messages and you
never touch that stuff again.

amqp-simple-connection-manager will reconnect to a new broker whenever the broker it is
currently connected to dies. When you ask amqp-simple-connection-manager for a
channel, you specify one or more `setup` functions to run; the setup functions
will be run every time amqp-simple-connection-manager reconnects, to make sure your
channel and broker are in a sane state.

Before we get into an example, note this example is written using Promises,
however much like amqplib, any function which returns a Promise will also accept
a callback as an optional parameter.

Here's the example:

```ts
import * as amqp from '@hcon/rabbit-fire';

// Create a new connection manager
const connection = amqp.connect(['amqp://localhost']);

// Ask the connection manager for a ChannelWrapper.  Specify a setup function to
// run every time we reconnect to the broker.
const channelWrapper = connection.createChannel({
  name: 'chann',
  async setup(channel: amqp.Channel) {
    await channel.assertExchange('chann.ex', 'direct', {
      durable: true,
    });
  },
});

// Send some messages to the queue.  If we're not currently connected, these will be queued up in memory
// until we connect.  Note that `publish()` return a Promise which is fulfilled or rejected
// without confirm from broker.
channelWrapper
  .publish('chann.ex', Buffer.from('hello world'))
  .then(function () {
    return console.log('Message was sent!  Hooray!');
  })
  .catch(function (err) {
    return console.log('Message was rejected...  Boo!');
  });
```

## API

### connect(urls, options)

Creates a new AmqpConnectionManager, which will connect to one of the URLs provided in `urls`. If a broker is
unreachable or dies, then AmqpConnectionManager will try the next available broker, round-robin.

Options:

- `options.heartbeatIntervalInSeconds` - Interval to send heartbeats to broker. Defaults to 5 seconds.
- `options.reconnectTimeInSeconds` - The time to wait before trying to reconnect. If not specified,
  defaults to `heartbeatIntervalInSeconds`.
- `options.connectionOptions` is passed as options to the amqplib connect method.

### AmqpConnectionManager events

- `connect({connection, url})` - Emitted whenever we successfully connect to a broker.
- `connectFailed({err, url})` - Emitted whenever we attempt to connect to a broker, but fail.
- `disconnect({err})` - Emitted whenever we disconnect from a broker.
- `blocked({reason})` - Emitted whenever a connection is blocked by a broker
- `unblocked` - Emitted whenever a connection is unblocked by a broker

### AmqpConnectionManager#createChannel(options)

Create a new ChannelWrapper. This is a proxy for the actual channel (which may or may not exist at any moment,
depending on whether or not we are currently connected.)

Options:

- `options.name` - Name for this channel. Used for debugging.
- `options.setup(channel, [cb])` - A function to call whenever we reconnect to the
  broker (and therefore create a new underlying channel.) This function should
  either accept a callback, or return a Promise. See `addSetup` below.
  Note that `this` inside the setup function will the returned ChannelWrapper.
  The ChannelWrapper has a special `context` member you can use to store
  arbitrary data in.

### AmqpConnectionManager#isConnected()

Returns true if the AmqpConnectionManager is connected to a broker, false otherwise.

### AmqpConnectionManager#close()

Close this AmqpConnectionManager and free all associated resources.

### ChannelWrapper events

- `connect` - emitted every time this channel connects or reconnects.
- `error(err, {name})` - emitted if an error occurs setting up the channel.
- `close` - emitted when this channel closes via a call to `close()`


### ChannelWrapper#publish

Work exactly like the counterparts in amqplib's Channel, except that they return a Promise (or accept a
callback) which resolves when the message is confirmed to have been delivered to the broker. The promise rejects if there is an unexpected error or if `close()` is called on the ChannelWrapper before the message can be
delivered.

### ChannelWrapper#ack and ChannelWrapper#nack

These are just aliases for calling `ack()` and `nack()` on the underlying channel. They do nothing if the underlying
channel is not connected.

### ChannelWrapper#queueLength()

Returns a count of messages currently waiting to be sent to the underlying channel.

### ChannelWrapper#close()

Close a channel, clean up resources associated with it.
