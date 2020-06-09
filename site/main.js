const logger = require('./logger');
const { getPortOfPeer, getSiteName } = require('./arch');

const port = getPortOfPeer(getSiteName())
if (!port) {
	logger.error("Invalid site");
	process.exit(1);
}

const { transact, getTransactionState } = require('./coordinator');
const { db, saveTransaction, prepareTransaction, commitTransaction, abortTransaction, setCoordinator } = require('./participant');

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

app.get('/coordinator/tstate/:tid', (req, res) => {
	res.send(getTransactionState(req.params.tid));
});

app.post('/save/:tid', (req, res) => {
	setCoordinator(req.params.tid, req.headers.origin);
	saveTransaction(req.params.tid, req.body);
	res.send("DONE");
});

app.get('/prepare/:tid', (req, res) => {
	if (prepareTransaction(req.params.tid)) {
		res.send("READY");
	}
	else {
		res.sendStatus(status.INTERNAL_SERVER_ERROR);
	}
});

app.post('/commit/:tid', (req, res) => {
	if (commitTransaction(req.params.tid)) {
		res.send("ACK");
	}
	else {
		res.sendStatus(status.INTERNAL_SERVER_ERROR);
	}
});

app.post('/abort/:tid', (req, res) => {
	setTimeout(() => res.send("ACK"), randInt(1000));
	abortTransaction(req.params.tid);
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
