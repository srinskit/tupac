const { createLogger, format, transports } = require('winston');

const logger = createLogger({
	level: 'debug',
	format: format.combine(
		format.prettyPrint(),
		format.timestamp({ format: 'hh:mm:ss' }),
		format.cli(),
		format.printf(info => `${info.timestamp} ${info.level}:${info.message}`)
	),
	transports: [new transports.Console()]
});

const { create, all } = require('mathjs');
const math = create(all, {});

const db = {};
const transactionLog = {};
const pendingCommits = {};

function saveTransaction(tid, transaction) {
	transactionLog[tid] = transaction;
	logger.info("Transaction received");
	logger.info("Transaction logs: ");
	console.log(transactionLog);
	console.log();
}

function prepareTransaction(tid) {
	logger.info(`Preparing transaction ${tid}`);

	const { instructions } = transactionLog[tid];
	const state = clone(db);
	math.evaluate(instructions, state)
	pendingCommits[tid] = state;

	logger.info("Pending commits:");
	console.log(pendingCommits);
	console.log();
}

function commitTransaction(tid) {
	logger.info("DB before commit: " + JSON.stringify(db));
	logger.info(`Committing transaction ${tid}`);

	commit = pendingCommits[tid];
	for (let variable in commit) {
		db[variable] = commit[variable];
	}
	delete transactionLog[tid];
	delete pendingCommits[tid];

	logger.info("DB after commit: " + JSON.stringify(db));
}

function rollbackTransaction(tid) {
	logger.info(`Rolling-back transaction ${tid}`);

	delete transactionLog[tid];
	delete pendingCommits[tid];

	logger.info("Transaction logs:");
	console.log(transactionLog);
	console.log();
	logger.info("Pending commits:");
	console.log(pendingCommits);
	console.log();
}

const port = getPortOfPeer(process.argv[2])
if (!port) {
	logger.error("Invalid site");
	process.exit(1);
}

const express = require('express');
const status = require('http-status');

const app = express();

app.set('port', port);
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.post('/transact/:tid', (req, res) => {
	setTimeout(() => res.send("DONE"), randInt(1000));
	saveTransaction(req.params.tid, req.body);
});

app.get('/query_to_commit/:tid', (req, res) => {
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

app.get('/db', (req, res) => {
	res.json(db);
});

const server = require('http').createServer(app);
server.listen(port);
server.on('listening', function () {
	const { port } = server.address();
	logger.info('Started server. Port:' + port);
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

function clone(simpleObj) {
	return JSON.parse(JSON.stringify(simpleObj));
}