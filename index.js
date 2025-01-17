require('dotenv').config();
const env = process.env.NODE_ENV;
const pixiotaApiPort = process.env.PIXIOTA_API_PORT;
const iotaProviders = process.env.IOTA_PROVIDERS;
const mongoDbUrl = process.env.MONGODB_URL;
const mongoDbName = process.env.MONGODB_NAME;
const redisFieldName = process.env.REDIS_FIELD_NAME;
const zmqUrls = process.env.ZMQ_URLS;
const zmqLocalBindPort = process.env.ZMQ_LOCAL_BIND_PORT;

const command = process.argv.slice(2)[0];
const developers = require("./developers.json");
const MongoClient = require('mongodb').MongoClient;
const WebSocket = require('ws');
const IOTA = require('iota.lib.js');
const zmq = require('zeromq');
const express = require('express');
const expressApp = express();
const expressCompression = require('compression');
const expressCacheResponseDirective = require('express-cache-response-directive');
const zmqXSubscriber = zmq.socket('xsub');
const zmqXPublisher = zmq.socket('xpub');
const zmqSubscriber = zmq.socket('sub');
const redis = require("redis");
const redisClient = redis.createClient({detect_buffers: true});
const boardSize = 256;
const expressServer = expressApp.listen(pixiotaApiPort, () => {
    console.log(`Express started on port ${pixiotaApiPort}`)
});
const wss = new WebSocket.Server({server: expressServer});

let client = null;
let db = null;
let iotaServices = [];
let iota = null;
for (i in iotaProviders.split(";")) {
    let iotaProvider = iotaProviders.split(";")[i];
    iota = new IOTA({
        provider: iotaProvider,
    });
    iotaServices.push(iota);
    console.log(`Registered iota provider: ${iotaProvider}`);
}

redisClient.on("error", err => {
    console.log("REDIS Error " + err);
});

// broadcast to all websocket clients
wss.broadcast = data => {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(data);
        }
    });
};

function trytesToMessage(trytes) {
    const message = iota.utils.fromTrytes(
        trytes.replace(/9+$/, "")
    );
    if (!message) return null;
    return message.replace(/[^a-zA-Z0-9\s\\.]+/g, "")
}

function messageToPixel(message, value = "0", id = "", to = "") {
    if (!message) return null;
    const pixelData = ((args) => {
        if (args.length < 2) return null;

        const pixelArray = args[1].split('.');
        return {
            type: 'transaction',
            a: parseInt(value),
            c: parseInt(pixelArray[0], 16),
            x: parseInt(pixelArray[1], 36),
            y: parseInt(pixelArray[2], 36),
            f: id,
            t: to,
        }
    })(message.split(' '));
    if (!pixelData ||
        pixelData.x < 0 || pixelData.x >= boardSize ||
        pixelData.y < 0 || pixelData.y >= boardSize ||
        pixelData.c < 0 || pixelData.c >= 16)
        return null;
    return pixelData;
}

function recoverMissingPixels() {
    let donationAddresses = [].concat.apply([], developers.map(dev => {
        return [dev.address].concat(dev.spentAddresses || []);
    }));

    iotaWrapper("findTransactionObjects", {
        addresses: donationAddresses,
    }, (err, transactions) => {
        iotaWrapper("getLatestInclusion", transactions.map(t => t.hash), (err, inclusions) => {
            transactions.forEach(transaction => {
                const message = trytesToMessage(transaction.signatureMessageFragment);
                transaction.pixelMessage = message;
                transaction.pixelData = messageToPixel(message);
            });
            const confirmedTransactions = transactions
                .filter((tr, i) => inclusions[i])
                .filter(tr => tr.value > 0)
                .filter(tr => tr.pixelData !== null)
                .sort((a, b) => a.attachmentTimestamp - b.attachmentTimestamp)
            ;

            db.collection('transactions').find({id: {$in: confirmedTransactions.map(t => t.hash)}}).toArray()
                .catch(err => {
                    console.log(err);
                    console.log("ERROR");
                })
                .then(results => {
                    if (results.length !== confirmedTransactions.length) {
                        let missingTransactions = [];

                        confirmedTransactions.forEach(transaction => {
                            let found = false;
                            for (const result of results) {
                                if (result.id === transaction.hash) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) missingTransactions.push(transaction);
                        });

                        missingTransactions.forEach(transaction => {
                            pixiotaDispatchPixel(transaction.pixelMessage, transaction.value, transaction.hash, transaction.address, null, transaction.attachmentTimestamp);
                        });

                        if (missingTransactions.length > 0)
                            console.log(`Recovered ${missingTransactions.length} missing transactions via polling.`);
                    }
                    setTimeout(recoverMissingPixels, 20000);
                });
        });
    });
}

setTimeout(recoverMissingPixels, 1000);

function pixiotaDispatchPixel(message, value, id, to, milestone, attachmentTimestamp) {
    if (!message.startsWith("pixiota ")) return;
    const pixelData = messageToPixel(message, value, id, to);
    if (!pixelData) return;

    db.collection('transactions').insertOne({
        milestone: milestone,
        id: id,
        to: to,
        value: pixelData.a,
        x: pixelData.x,
        y: pixelData.y,
        color: pixelData.c,
        attachmentTimestamp: parseInt(attachmentTimestamp),
    });
    redisClient.bitfield([redisFieldName, "SET", "u4", `#${pixelData.y * boardSize + pixelData.x}`, pixelData.c]);
    wss.broadcast(JSON.stringify(pixelData))
}

// ws heartbeat
setInterval(() => {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) return ws.terminate();

        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('connection', ws => {
    ws.isAlive = true;
    ws.on('pong', () => {
        ws.isAlive = true;
    });

    if (!db) return;

    // send last 10 transactions
    db.collection('transactions').find({}).limit(10).sort({attachmentTimestamp: -1}).toArray()
        .catch(err => {
            console.log(err);
            console.log("ERROR");
        })
        .then(results => {
            if (ws.readyState !== WebSocket.OPEN) return;
            ws.send(JSON.stringify({
                type: "latest_transactions",
                transactions: results.map((result) => {
                    return {
                        a: result.value,
                        c: result.color,
                        x: result.x,
                        y: result.y,
                        f: result.id,
                        t: result.to,
                    };
                }),
            }))
        });
});

function iotaWrapper(method, params, callback, _services) {
    if (_services === undefined)
        _services = iotaServices.slice(0);
    if (_services.length === 0) {
        console.log(`iotaWrapper: "${method} failed with every services."`);
        return callback(null, []); // dummy callback
    }
    _services[0].api[method](params, (err, data) => {
        if (err) {
            return iotaWrapper(method, params, callback, _services.slice(1));
        }
        return callback(err, data);
    })
};

MongoClient.connect(mongoDbUrl)
    .then(_client => {
        client = _client;
        db = client.db(mongoDbName);

        if (command === "clear") {
            db.collection('transactions').remove({}, (err, numberRemoved) => {
                console.log(`Removed ${numberRemoved.result.n} transactions from database`);
                redisClient.set([redisFieldName, ""], (err, reply) => {
                    console.log("Reset redis map --> OK");
                    console.log("Clear successful. Exiting.");
                    process.exit();
                });
            });
        }

        if (command === "recover") {
            let donationAddresses = [].concat.apply([], developers.map(dev => {
                return [dev.address].concat(dev.spentAddresses || []);
            }));
            iotaWrapper("findTransactionObjects", {
                addresses: donationAddresses,
            }, (err, transactions) => {
                iotaWrapper("getLatestInclusion", transactions.map(t => t.hash), (err, inclusions) => {
                    const confirmedTransactions = transactions
                        .filter((tr, i) => inclusions[i])
                        .filter(tr => tr.value > 0)
                        .sort((a, b) => a.attachmentTimestamp - b.attachmentTimestamp)
                    ;

                    let runningQueries = 0;
                    confirmedTransactions.forEach(transaction => {
                        const message = trytesToMessage(transaction.signatureMessageFragment);
                        const pixelData = messageToPixel(message);
                        if (!pixelData) return;
                        runningQueries++;
                        db.collection('transactions').updateOne(
                            {id: transaction.hash},
                            {
                                "$set": {
                                    id: transaction.hash,
                                    to: transaction.address,
                                    value: transaction.value,
                                    x: pixelData.x,
                                    y: pixelData.y,
                                    color: pixelData.c,
                                    attachmentTimestamp: parseInt(transaction.attachmentTimestamp),
                                }
                            },
                            (err, res) => {
                                if (res.result.n > 0) {
                                    runningQueries--;
                                } else {
                                    db.collection('transactions').insertOne({
                                        milestone: null,
                                        id: transaction.hash,
                                        to: transaction.address,
                                        value: transaction.value,
                                        x: pixelData.x,
                                        y: pixelData.y,
                                        color: pixelData.c,
                                        attachmentTimestamp: parseInt(transaction.attachmentTimestamp),
                                    }, (err, res) => {
                                        runningQueries--;
                                    });
                                }
                            });
                        redisClient.bitfield([redisFieldName, "SET", "u4", `#${pixelData.y * boardSize + pixelData.x}`, pixelData.c]);
                    });
                    setInterval(() => {
                        if (runningQueries <= 0) {
                            // Wait for every mongodb update to finish
                            console.log("Recovering data --> OK");
                            console.log("Recover successful. Exiting.");
                            process.exit();
                        }
                    }, 200);
                })
            })
        }
    })
;

// forces compression for every Express route (including binary map) (FIXME: Only for binary map?)
expressApp.use(expressCompression({filter: (req, res) => true}));
expressApp.use(expressCacheResponseDirective());
expressApp.get('/map', (req, res) => {
    redisClient.get(new Buffer(redisFieldName), (err, map) => {
        if (env === "dev")
            res.setHeader('Access-Control-Allow-Origin', '*');
        res.cacheControl({maxAge: 1, staleWhileRevalidate: 1}); // 1-second cache only
        res.header("Content-Type", "application/octet-stream");
        res.end(map, 'binary');
    });
});
expressApp.get('/projects', (req, res) => {
    if (env === "dev")
        res.setHeader('Access-Control-Allow-Origin', '*');
    res.cacheControl({maxAge: 1800, staleWhileRevalidate: 60}); // 1800-second cache
    res.json(developers.map(dev => {
        return {
            address: dev.address,
            project: dev.project,
            link: dev.link,
        };
    }));
});
expressApp.get('/', (req, res) => {
    res.cacheControl({maxAge: 3600, staleWhileRevalidate: 3600});
    res.send("Hmm... May I help you? ;)")
});

// Use connect method to connect to the server
MongoClient.connect(mongoDbUrl)
    .catch(err => {
        console.log("An error occured while connecting to MongoDB! Exiting");
        console.log(err);
        process.exit(1);
    })
    .then(_client => {
        client = _client;
        db = client.db(mongoDbName);

        const urls = zmqUrls.split(";");
        zmqXPublisher.bindSync(`tcp://*:${zmqLocalBindPort}`);
        urls.forEach(url => {
            console.log(`XSUB connecting to: ${url}`);
            zmqXSubscriber.connect(url);
        });
        zmqSubscriber.connect(`tcp://127.0.0.1:${zmqLocalBindPort}`).subscribe('sn ');
    })
;

zmqXPublisher.on('message', msg => {
    zmqXSubscriber.send(msg);
});

function isDeveloper(addr) {
    for (let i in developers) {
        if (developers[i].address.startsWith(addr))
            return true;
    }
    return false;
}

let msgProxy = [];
zmqXSubscriber.on('message', msg => {
    const smsg = msg.toString();
    const [, , to] = smsg.split(' ').slice(1);

    // if message has been handled recently, ignore it
    if (!isDeveloper(to) || msgProxy.indexOf(smsg) > -1) return;

    msgProxy.unshift(smsg);
    if (msgProxy.length > 4000)
        msgProxy.splice(-1, 1);
    zmqXPublisher.send(msg); // Forward message using the xpub so subscribers can receive it
});

zmqSubscriber.on('message', msg => {
    const [milestone, id, to] = msg.toString().split(' ').slice(1);
    if (!isDeveloper(to)) return;

    console.log("milestone", milestone, "id", id, "to", to);

    iotaWrapper("getTransactionsObjects", [id],
        (err, results) => {
            if (err)
                console.log(err);
            if (!results || results.length <= 0) {
                console.error("Well... this shouldn't happen");
                return;
            }
            let transaction = results[0];
            const message = trytesToMessage(transaction.signatureMessageFragment);
            console.log(message);
            let value = transaction.value;
            if (value === 0) {
                // ignore 0-value transactions
                return;
            } else if (value < 0) {
                // TODO: Catch when a donation address is spent
                console.log("Something really really bad happened!");
                return;
            }

            pixiotaDispatchPixel(message, value, id, to, milestone, transaction.attachmentTimestamp);
        });
});
zmqSubscriber.on('close', () => {
    console.log("ZMQ connection lost :(")
});
