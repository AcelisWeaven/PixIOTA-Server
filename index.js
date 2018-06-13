require('dotenv').config();
const env = process.env.NODE_ENV;
const pixiotaApiPort = process.env.PIXIOTA_API_PORT;
const iotaProvider = process.env.PIXIOTA_PROVIDER;
const mongoDbUrl = process.env.MONGODB_URL;
const mongoDbName = process.env.MONGODB_NAME;
const zmqUrl = process.env.ZMQ_URL;

const MongoClient = require('mongodb').MongoClient;
const WebSocket = require('ws');
const IOTA = require('iota.lib.js');
const zmq = require('zeromq');
const express = require('express');
const expressApp = express();
const expressCompression = require('compression');
const expressCacheResponseDirective = require('express-cache-response-directive');
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
let iota = new IOTA({
    provider: iotaProvider,
});

redisClient.on("error", function (err) {
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


function pixiotaDispatchPixel(message, value, id, to, milestone) {
    if (!message.startsWith("pixiota ")) return;
    const pixelData = ((args) => {
        if (args.length < 2) return null;

        const pixelArray = args[1].split('.');
        return {
            type: 'transaction',
            a: parseInt(value),
            c: parseInt(pixelArray[0]),
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
        return;

    db.collection('transactions').insertOne({
        milestone: milestone,
        id: id,
        to: to,
        value: pixelData.a,
        x: pixelData.x,
        y: pixelData.y,
        color: pixelData.c,
    });
    redisClient.bitfield(["map", "SET", "u4", `#${pixelData.y * boardSize + pixelData.x}`, pixelData.c]);
    wss.broadcast(JSON.stringify(pixelData))
}

// ws heartbeat
setInterval(function ping() {
    wss.clients.forEach(function each(ws) {
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
    db.collection('transactions').find({}).limit(10).sort({milestone: -1}).toArray()
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

// debug
MongoClient.connect(mongoDbUrl)
    .then(_client => {
        client = _client;
        db = client.db(mongoDbName);

        db.collection('transactions').remove({}, function (err, numberRemoved) {
            console.log(`DEBUG: Removed ${numberRemoved.result.n} transactions from database`);
            redisClient.set(["map", ""], (err, reply) => {
                console.log("DEBUG: Reset redis map --> OK");

                // for (let i = 0; i < boardSize * 20; i++) {
                //     pixiotaDispatchPixel(`pixiota ${Math.floor(Math.random() * 16)}.${Math.floor(i / boardSize).toString(36)}.${(i % boardSize).toString(36)}`, "2",
                //         "DVNMLPXKBBOIFHLVUNCFOPIIT9GJKADRRJYSDGHDIHCBGDEWYIPPUVQBDQRREGGYSPZ9VXPRXIXIA9999",
                //         "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                //         Math.floor(Math.random() * 100000));
                // }
                // setInterval(() => {
                //     pixiotaDispatchPixel(`pixiota ${Math.floor(Math.random() * 16)}.${Math.floor(Math.random() * boardSize).toString(36)}.${Math.floor(Math.random() * boardSize).toString(36)}`, "2",
                //         "DVNMLPXKBBOIFHLVUNCFOPIIT9GJKADRRJYSDGHDIHCBGDEWYIPPUVQBDQRREGGYSPZ9VXPRXIXIA9999",
                //         "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                //         Math.floor(Math.random() * 100000));
                // }, 1000);
                // console.log("Loaded sample pixels");
            });
        });
    })
;

// forces compression for every Express route (including binary map) (FIXME: Only for binary map?)
expressApp.use(expressCompression({filter: (req, res) => true}));
expressApp.use(expressCacheResponseDirective());
expressApp.get('/map', (req, res) => {
    redisClient.get(new Buffer("map"), (err, map) => {
        if (env === "dev")
            res.setHeader('Access-Control-Allow-Origin', '*');
        res.cacheControl({maxAge: 1, staleWhileRevalidate: 1}); // 1-second cache only
        res.end(map, 'binary');
    });
});
expressApp.get('/', (req, res) => {
    res.cacheControl({maxAge: 3600, staleWhileRevalidate: 3600});
    res.send("Hmm... May I help you?")
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

        zmqSubscriber.connect(zmqUrl).subscribe('sn ');
    })
;

zmqSubscriber.on('message', function (msg) {
    let [milestone, id, to] = msg.toString().split(' ').slice(1);

    if (to !== "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        return;
    console.log("milestone", milestone, "id", id, "to", to);

    iota.api.getTransactionsObjects([id],
        (err, results) => {
            if (err)
                console.log(err);
            if (!results || results.length <= 0) {
                console.error("Well... this shouldn't happen");
                return;
            }
            let transaction = results[0];
            let message = iota.utils.fromTrytes(
                transaction.signatureMessageFragment.replace(/9+$/, "")
            ).replace(/[^a-zA-Z0-9\s\\.]+/g, "");
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

            pixiotaDispatchPixel(message, value, id, to, milestone);
        });
});
zmqSubscriber.on('close', function () {
    console.log("ZMQ connection lost :(")
});
