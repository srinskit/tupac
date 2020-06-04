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

const FAILURE = 0, SUCCESS = 1, TIMEOUT = -1;
const TIMOUT_TIME = 5000;
const sites = ['site1', 'site2', 'site3', 'site4'];
const timers = {};
const client_res = {};
const done_remaining = {};
const ready_remaining = {};
const commit_ack_remaining = {};
const rollback_ack_remaining = {};

function transact(tid, transactionData) {
	done_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`Sending transaction to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/transact/${tid}`, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json'
			},
			body: transactionData
		}), TIMOUT_TIME)
			.then(({ ok }) => handleTransactResult(resultify(ok), tid, site))
			.catch(({ message }) => handleTransactResult(resultify(message), tid, site));
	});
}

function handleTransactResult(result, tid, site) {
	if (!done_remaining[tid]) return;

	if (result === SUCCESS) {
		done_remaining[tid]--;
		logger.info(`${site} DONE processing ${tid}`);
		if (done_remaining[tid] === 0) {
			logger.info(`All sites DONE processing ${tid}`);
			process.nextTick(() => queryToCommit(tid))
		}
	} else {
		done_remaining[tid] = 0;
		const msg = `${site} failed to process ${tid}${result === TIMEOUT ? ', timed-out' : ''}`
		logger.error(msg);
		handleTransactionComplete(tid, result, msg);
	}
}

function queryToCommit(tid) {
	ready_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`Asking ${site} if READY to commit ${tid}`);
		timeout(fetch(`${getUrlOfPeer(site)}/query_to_commit/${tid}`), TIMOUT_TIME)
			.then(({ ok }) => handleQueryToCommitResult(resultify(ok), tid, site))
			.catch(({ message }) => handleQueryToCommitResult(resultify(message), tid, site));
	});
}

function handleQueryToCommitResult(result, tid, site) {
	if (!ready_remaining[tid]) return;

	if (result === SUCCESS) {
		ready_remaining[tid]--;
		logger.info(`${site} READY to commit ${tid}`);
		if (ready_remaining[tid] === 0) {
			logger.info(`All sites READY to commit ${tid}`);
			process.nextTick(() => askToCommit(tid));
		}
	}
	else {
		ready_remaining[tid] = 0;
		const msg = `${site} NOT READY to commit ${tid}${result === TIMEOUT ? ', timed-out' : ''}`;
		logger.error(msg);
		process.nextTick(() => askToRollback(tid));
	}
}

function askToCommit(tid) {
	commit_ack_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`Asking ${site} to commit ${tid}`);
		timeout(fetch(`${getUrlOfPeer(site)}/commit/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleCommitResult(resultify(ok), tid, site))
			.catch(({ message }) => handleCommitResult(resultify(message), tid, site));
	});
}

function handleCommitResult(result, tid, site) {
	if (!commit_ack_remaining[tid]) return;

	if (result === SUCCESS) {
		commit_ack_remaining[tid]--;
		logger.info(`${site} COMMITED ${tid}`);
		if (commit_ack_remaining[tid] === 0) {
			logger.info(`All sites COMMITED ${tid}`);
			process.nextTick(() => handleTransactionComplete(tid, SUCCESS))
		}
	}
	else {
		commit_ack_remaining[tid] = 0;
		const msg = `${site} failed to COMMIT ${tid}${result === TIMEOUT ? ', timed-out' : ''}`;
		logger.error(msg);
		handleTransactionComplete(tid, result, msg);
	}
}

function askToRollback(tid) {
	rollback_ack_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`Asking ${site} to rollback ${tid}`);
		timeout(fetch(`${getUrlOfPeer(site)}/rollback/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleRollbackResult(resultify(ok), tid, site))
			.catch(({ message }) => handleRollbackResult(resultify(message), tid, site));
	});
}

function handleRollbackResult(result, tid, site) {
	if (!rollback_ack_remaining[tid]) return;

	if (result === SUCCESS) {
		rollback_ack_remaining[tid]--;
		logger.info(`${site} ROLLEDBACK ${tid}`);
		if (rollback_ack_remaining[tid] === 0) {
			logger.info(`All sites ROLLEDBACK ${tid}`);
			handleTransactionComplete(tid, FAILURE, 'atleast one site NOT READY');
		}
	}
	else {
		rollback_ack_remaining[tid] = 0;
		const msg = `${site} failed to ROLLBACK ${tid}${result === TIMEOUT ? ', timed-out' : ''}`;
		logger.error(msg);
		handleTransactionComplete(tid, result, msg);
	}
}

function handleTransactionComplete(tid, result, reason) {
	const res = client_res[tid];
	if (result === SUCCESS) {
		res.end("SUCCESS\n");
	}
	else {
		res.end(`FAILED: ${reason}\n`);
	}
}

const port = getPortOfPeer(process.argv[2])
if (!port) {
	logger.error("Invalid site");
	process.exit(1);
}

const express = require('express');
const morgan = require('morgan');
const status = require('http-status');
const { v1: uuidv1 } = require('uuid');
const fetch = require('node-fetch');

const app = express();

app.set('port', port);
app.use(morgan('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.post('/transact', (req, res) => {
	const transaction = req.body;
	const transactionData = JSON.stringify(transaction);
	const tid = uuidv1();

	res.write(`Transaction ID: ${tid}\n`);
	res.write(`Transaction: ${transactionData}\n`);
	client_res[tid] = res;

	transact(tid, transactionData);

	timers[tid] = setTimeout(() => {
	}, 5000);
});
app.post('/vote/:tid/:action', (req, res) => res.sendStatus(status.OK));
app.post('/ack/:tid', (req, res) => res.sendStatus(status.OK));

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

function getUrlOfPeer(name) {
	return `http://localhost:${getPortOfPeer(name)}`
}

function timeout(promise, ms) {
	return new Promise((resolve, reject) => {
		const timeoutId = setTimeout(() => {
			reject(new Error("timeout"))
		}, ms);
		promise.then(
			(res) => {
				clearTimeout(timeoutId);
				resolve(res);
			},
			(err) => {
				clearTimeout(timeoutId);
				reject(err);
			}
		);
	})
}

function resultify(result) {
	switch (result) {
		case true: return SUCCESS;
		case 'timeout': return TIMEOUT;
		default: return FAILURE;
	}
}