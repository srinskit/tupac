const { createLogger, format, transports } = require('winston');

const logger = createLogger({
	level: 'debug',
	format: format.combine(
		format.timestamp({ format: 'YYYY-MM-DD hh:mm:ss' }),
		format.cli(),
		format.printf(info => `${info.timestamp} ${info.level}:${info.message}`)
	),
	transports: [new transports.Console()]
});

const FAILURE = 0, SUCCESS = 1, TIMEOUT = -1;
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
		fetch(`${getUrlOfPeer(site)}/transact/${tid}`, {
			method: 'POST',
			body: transactionData
		})
			.then(r => handleTransactResult(r.ok ? SUCCESS : FAILURE, tid, site))
			.catch(_ => handleTransactResult(FAILURE, tid, site));
	});
}

function handleTransactResult(result, tid, site) {
	if (result === SUCCESS) {
		logger.info(`Received DONE from ${site} for ${tid}`);
		done_remaining[tid]--;
		if (done_remaining[tid] === 0) {
			logger.info(`All sites DONE processing ${tid}`);
			process.nextTick(() => queryToCommit(tid))
		}
	} else if (result === FAILURE) {
		logger.error(`Received ERROR from ${site} for ${tid}`);
	} else if (result === TIMEOUT) {
	}
}

function queryToCommit(tid) {
	ready_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`Asking ${site} if READY to commit ${tid}`);
		fetch(`${getUrlOfPeer(site)}/query_to_commit/${tid}`)
			.then(r => handleQueryToCommitResult(r.ok ? SUCCESS : FAILURE, tid, site))
			.catch(_ => handleQueryToCommitResult(FAILURE, tid, site));
	});
}

function handleQueryToCommitResult(result, tid, site) {
	if (result === SUCCESS) {
		logger.info(`Received READY from ${site} for ${tid}`);
		ready_remaining[tid]--;
		if (ready_remaining[tid] === 0) {
			logger.info(`All sites READY to commit ${tid}`);
			process.nextTick(() => askToCommit(tid));
		}
	}
	else if (result === FAILURE) {
		logger.error(`Received NOT READY from ${site} for ${tid}`);
	}
	else if (result === TIMEOUT) {
	}
}

function askToCommit(tid) {
	commit_ack_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`Asking ${site} to commit ${tid}`);
		fetch(`${getUrlOfPeer(site)}/commit/${tid}`, { method: 'POST' })
			.then(r => handleCommitResult(r.ok ? SUCCESS : FAILURE, tid, site))
			.catch(e => handleCommitResult(FAILURE, tid, site));
	});
}

function handleCommitResult(result, tid, site) {
	if (result === SUCCESS) {
		logger.info(`Received COMMIT ACK from ${site} for ${tid}`);
		commit_ack_remaining[tid]--;
		if (commit_ack_remaining[tid] === 0) {
			logger.info(`All sites COMMITED ${tid}`);
			const res = client_res[tid];
			res.end("DONE\n");
		}
	}
	else if (result === FAILURE) {
	}
	else if (result === TIMEOUT) {
	}
}

function askToRollback(tid) {
	rollback_ack_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`Asking ${site} to rollback ${tid}`);
		fetch(`${getUrlOfPeer(site)}/rollback/${tid}`, { method: 'POST' })
			.then(r => handleRollbackResult(r.ok ? SUCCESS : FAILURE, tid, site))
			.catch(e => handleRollbackResult(FAILURE, tid, site));
	});
}

function handleRollbackResult(result, tid, site) {
	if (result === SUCCESS) {
	}
	else if (result === FAILURE) {
	}
	else if (result === TIMEOUT) {
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