const env = process.env.NODE_ENV || 'dev';
const MongoClient = require('mongodb').MongoClient;
const IOTA = require('iota.lib.js');
const WebSocket = require('ws');
const zmq = require('zeromq');
const express = require('express');
const expressApp = express();
const expressCompression = require('compression');
const wss = new WebSocket.Server({port: 8081});
const subscriber = zmq.socket('sub');
const redis = require("redis");
const redisClient = redis.createClient({detect_buffers: true});
const url = 'mongodb://localhost:27017';
const dbName = 'pixiota';
const boardSize = 256;

let client = null;
let db = null;
let iota = new IOTA({
    'host': 'http://static.88-198-93-219.clients.your-server.de',
    'port': 14267
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

wss.on('connection', ws => {
    // send last 10 transactions
    db.collection('transactions').find({}).limit(10).sort({milestone: -1}).toArray()
        .catch(err => {
            console.log(err);
            console.log("ERROR");
        })
        .then(results => {
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
MongoClient.connect(url)
    .then(_client => {
        client = _client;
        db = client.db(dbName);

        db.collection('transactions').remove({}, function (err, numberRemoved) {
            console.log(`DEBUG: Removed ${numberRemoved.result.n} transactions from database`);
            redisClient.set(["map", ""], (err, reply) => {
                console.log("DEBUG: Reset redis map --> OK");

                for (let i = 0; i < boardSize * 30; i++) {
                    pixiotaDispatchPixel(`pixiota ${Math.floor(Math.random() * 16)}.${Math.floor(i / boardSize).toString(36)}.${(i % boardSize).toString(36)}`, "2",
                        "DVNMLPXKBBOIFHLVUNCFOPIIT9GJKADRRJYSDGHDIHCBGDEWYIPPUVQBDQRREGGYSPZ9VXPRXIXIA9999",
                        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                        Math.floor(Math.random() * 100000));
                }
                setInterval(() => {
                    pixiotaDispatchPixel(`pixiota ${Math.floor(Math.random() * 16)}.${Math.floor(Math.random() * boardSize).toString(36)}.${Math.floor(Math.random() * boardSize).toString(36)}`, "2",
                        "DVNMLPXKBBOIFHLVUNCFOPIIT9GJKADRRJYSDGHDIHCBGDEWYIPPUVQBDQRREGGYSPZ9VXPRXIXIA9999",
                        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                        Math.floor(Math.random() * 100000));
                }, 200);
                console.log("Loaded sample pixels");
            });
        });
    })
;

// forces compression for every Express route (including binary map)
expressApp.use(expressCompression({filter: (req, res) => true}));
expressApp.get('/map', function (req, res) {
    redisClient.get(new Buffer("map"), (err, map) => {
        if (env === "dev")
            res.setHeader('Access-Control-Allow-Origin', '*');
        res.cacheControl({maxAge: 1, staleWhileRevalidate: 1}); // 1-second cache only
        res.end(map, 'binary');
    });
});

expressApp.listen(3000, function () {
    console.log('Express started on port 3000')
});
return;

// Use connect method to connect to the server
MongoClient.connect(url)
    .catch(err => {
        console.log("An error occured! Exiting");
        console.log(err);
        process.exit(1);
    })
    .then(_client => {
        console.log("Connected successfully to server");
        client = _client;
        db = client.db(dbName);

        // Empty db
        // db.collection('transactions').remove({},function(err,numberRemoved){
        //     console.log("inside remove call back" + numberRemoved);
        // });
        startZMQsubscriber();
        // return db.collection('transactions').insertOne({
        //     milestone: 520700,
        //     id: "HJAJOFIHZOZASKLUQVTWSTQMYYKKVRJAQDDBYFOAPVVLNPRZEBGGHE9VQWELDQVQBFNFQBRJYCDHA9999",
        //     to: "AAJXXFJUEQHKPYIOUIUO9FWCMOAFBZAZPXIFRI9FLDQZJGHQENG9HNMODUZJCHR9RHHUSBHWJELGRDOWZ",
        //     value: undefined
        // });
    })
;

subscriber.on('message', function (msg) {
    let [milestone, id, to] = msg.toString().split(' ').slice(1);

    if (to !== "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        return;
    console.log("milestone", milestone, "id", id, "to", to);

    iota.api.getTransactionsObjects([id],
        (err, results) => {
            if (err)
                console.log(err);
            if (results.length <= 0) {
                console.error("Well... this shouldn't happen");
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
subscriber.on('close', function () {
    console.log("ZMQ connection lost :(")
});

function startZMQsubscriber() {
    subscriber
        .connect('tcp://static.88-198-93-219.clients.your-server.de:5557')
        .subscribe('sn ');
}
