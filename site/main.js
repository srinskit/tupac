const logger = require('./logger');
const { getPortOfPeer } = require('./arch');

const port = getPortOfPeer(process.argv[2])
if (!port) {
	logger.error("Invalid site");
	process.exit(1);
}

const { transact } = require('./coordinator');
const { db, saveTransaction, prepareTransaction, commitTransaction, rollbackTransaction } = require('./participant');

const express = require('express');
const app = express();
const status = require('http-status');
const { v1: uuidv1 } = require('uuid');

app.set('port', port);
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.post('/coordinator/transact', (req, res) => {
	const transaction = req.body;
	const transactionData = JSON.stringify(transaction);
	const tid = uuidv1();

	res.write(`Transaction ID: ${tid}\n`);
	res.write(`Transaction: ${transactionData}\n`);

	transact(tid, transactionData, res);
});

app.post('/save/:tid', (req, res) => {
	setTimeout(() => res.send("DONE"), randInt(1000));
	saveTransaction(req.params.tid, req.body);
});

app.get('/prepare/:tid', (req, res) => {
	setTimeout(() => res.send("READY"), randInt(2500));
	prepareTransaction(req.params.tid);
});

app.post('/commit/:tid', (req, res) => {
	setTimeout(() => res.send("ACK"), randInt(1000));
	commitTransaction(req.params.tid);
});

app.post('/rollback/:tid', (req, res) => {
	setTimeout(() => res.send("ACK"), randInt(1000));
	rollbackTransaction(req.params.tid);
});

app.get('/db/:variable?', (req, res) => {
	const { variable } = req.params;
	if (variable) {
		res.json(db[variable]);
	}
	else {
		res.json(db);
	}
});

const server = require('http').createServer(app);
server.listen(port);
server.on('listening', function () {
	const { port } = server.address();
	logger.info('Started server. Port:' + port);
});

function randInt(max) {
	return Math.floor(Math.random() * max);
}
