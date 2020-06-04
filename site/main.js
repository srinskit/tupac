const { create, all } = require('mathjs');

const config = { }
const math = create(all, config)
const { createLogger, format, transports } = require('winston');

const logger = createLogger({
	level: 'debug',
	format: format.combine(
		format.timestamp({ format: 'hh:mm:ss' }),
		format.cli(),
		format.printf(info => `${info.timestamp} ${info.level}:${info.message}`)
	),
	transports: [new transports.Console()]
});

const transactionLog = {}

const db = { a: 0, b: 0, c: 0 };
var midCommit = {};

const port = getPortOfPeer(process.argv[2])
if (!port) {
	logger.error("Invalid site");
	process.exit(1);
}

const express = require('express');
const morgan = require('morgan');
const status = require('http-status');

const app = express();

app.set('port', port);
app.use(morgan('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.post('/transact/:tid', (req, res) => {
	setTimeout(() => res.send("DONE"), randInt(1000));
	saveTransaction(req);
});

function saveTransaction(req) {
	transactionLog[req.params.tid] = req.body;
	console.log("Transaction Received!");
	console.log(transactionLog);

	performTransaction(req);
}

function performTransaction(req) {
	transaction = transactionLog[req.params.tid];
	tra = transaction.instructions[0];
	trb = transaction.instructions[1];
	trc = transaction.instructions[2];

	console.log("Transactions:")
	parseTransaction(tra);
	parseTransaction(trb);
	parseTransaction(trc);

	console.log("Transaction completed locally!");
}

function parseTransaction(trans) {
	console.log(trans);
	math.evaluate(trans, midCommit);
}

app.get('/query_to_commit/:tid', (req, res) => {
	setTimeout(() => res.send("READY"), randInt(2500));
});
app.post('/commit/:tid', (req, res) => {
	setTimeout(() => res.send("ACK"), randInt(1000));
	commitTransaction();
});


function commitTransaction() {
	console.log("Object before Commit");
	console.log(db);
	console.log("Committing the Transactions!");
	db.a = midCommit.a;
	db.b = midCommit.b;
	db.c = midCommit.c;

	console.log("Object after Commit");
	console.log(db);
}

app.post('/rollback/:tid', (req, res) => {
	setTimeout(() => res.send("ACK"), randInt(1000));
	noCommit();
});

function noCommit(req) {
	const id = req.params.tid;
	console.log("Commit Failed ! Aborting the Transaction");
	delete transactionLog.id;
	console.log("Transaction Pool:");
	console.log(transactionLog);
}

const server = require('http').createServer(app);
server.listen(port);
server.on('listening', function () {
	const addr = server.address();
	const bind = typeof addr === 'string'
		? 'pipe ' + addr
		: 'port ' + addr.port;
	logger.info('Listening on ' + bind);
});

function getPortOfPeer(name) {
	return {
		'coordinator': 2000,
		'site1': 3000,
		'site2': 4000,
		'site3': 5000,
		'site4': 6000
	}[name];
}

function randInt(max) {
	return Math.floor(Math.random() * max);
}