const logger = require('./logger');
const { getUrlOfPeer, getMyUrl, getSiteName } = require('./arch');
const fetch = require('node-fetch');
const { saveTransaction, prepareTransaction, commitTransaction, abortTransaction } = require('./participant');

const FAILURE = 0, SUCCESS = 1, TIMEOUT = -1;
const TIMOUT_TIME = 5000;
const sites = ['site1', 'site2', 'site3', 'site4'].filter(s => s !== getSiteName());
const client_res = {};
const done_remaining = {};
const ready_remaining = {};
const commit_ack_remaining = {};
const abort_ack_remaining = {};
const logs = [];
const transactions = {};

function transact(tid, transactionData, res) {
	client_res[tid] = res;
	transactions[tid] = JSON.parse(transactionData);

	done_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`[COORD] Sending transaction to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/save/${tid}`, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Origin': getMyUrl()
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
		logger.info(`[COORD]: ${site} DONE processing ${tid}`);
		if (done_remaining[tid] === 0) {
			saveTransaction(tid, transactions[tid]);
			logger.info(`[COORD]: All sites DONE processing ${tid}`);
			process.nextTick(() => askToPrepare(tid))
		}
	} else {
		done_remaining[tid] = 0;
		const msg = `[COORD]: ${site} failed to process ${tid}${result === TIMEOUT ? ', timed-out' : ''}`
		logger.error(msg);
		handleTransactionComplete(tid, result, msg);
	}
}

function askToPrepare(tid) {
	logs.push({ type: 'prepare', tid });

	ready_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`[COORD]: Asking ${site} if READY to commit ${tid}`);
		timeout(fetch(`${getUrlOfPeer(site)}/prepare/${tid}`), TIMOUT_TIME)
			.then(({ ok }) => handlePrepareResult(resultify(ok), tid, site))
			.catch(({ message }) => handlePrepareResult(resultify(message), tid, site));
	});
}

function handlePrepareResult(result, tid, site) {
	if (!ready_remaining[tid]) return;

	if (result === SUCCESS) {
		ready_remaining[tid]--;
		logger.info(`[COORD]: ${site} READY to commit ${tid}`);
		if (ready_remaining[tid] === 0) {
			if (canExecute(transactions[tid], 'after-ready')) {
				prepareTransaction(tid);
				logger.info(`[COORD]: All sites READY to commit ${tid}`);
				process.nextTick(() => askToCommit(tid));
			} else {
				logger.error(`[COORD]: Failed. Recovering. ${tid}`);
				process.nextTick(() => recover(tid));
			}
		}
	}
	else {
		ready_remaining[tid] = 0;
		const msg = `[COORD]: ${site} NOT READY to commit ${tid}${result === TIMEOUT ? ', timed-out' : ''}`;
		logger.error(msg);
		process.nextTick(() => askToAbort(tid));
	}
}

function askToCommit(tid) {
	logs.push({ type: 'commit', tid });

	commit_ack_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`[COORD]: Asking ${site} to commit ${tid}`);
		timeout(fetch(`${getUrlOfPeer(site)}/commit/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleCommitResult(resultify(ok), tid, site))
			.catch(({ message }) => handleCommitResult(resultify(message), tid, site));
	});
}

function handleCommitResult(result, tid, site) {
	if (!commit_ack_remaining[tid]) return;

	if (result === SUCCESS) {
		commit_ack_remaining[tid]--;
		logger.info(`[COORD]: ${site} COMMITED ${tid}`);
		if (commit_ack_remaining[tid] === 0) {
			if (canExecute(transactions[tid], 'after-commit')) {
				commitTransaction(tid);
				logger.info(`[COORD]: All sites COMMITED ${tid}`);
				process.nextTick(() => handleTransactionComplete(tid, SUCCESS));
			}
			else {
				logger.error(`[COORD]: Failed. Recovering. ${tid}`);
				process.nextTick(() => recover(tid));
			}
		}
	}
	else {
		commit_ack_remaining[tid] = 0;
		const msg = `[COORD]: ${site} failed to COMMIT ${tid}${result === TIMEOUT ? ', timed-out' : ''}`;
		logger.error(msg);
		handleTransactionComplete(tid, result, msg);
	}
}

function askToAbort(tid) {
	logs.push({ type: 'abort', tid });

	abort_ack_remaining[tid] = sites.length;
	sites.forEach(site => {
		logger.info(`[COORD]: Asking ${site} to abort ${tid}`);
		timeout(fetch(`${getUrlOfPeer(site)}/abort/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleAbortResult(resultify(ok), tid, site))
			.catch(({ message }) => handleAbortResult(resultify(message), tid, site));
	});
}

function handleAbortResult(result, tid, site) {
	if (!abort_ack_remaining[tid]) return;

	if (result === SUCCESS) {
		abort_ack_remaining[tid]--;
		logger.info(`[COORD]: ${site} ABORTED ${tid}`);
		if (abort_ack_remaining[tid] === 0) {
			abortTransaction(tid);
			logger.info(`[COORD]: All sites ABORTED ${tid}`);
			handleTransactionComplete(tid, FAILURE, 'atleast one site NOT READY');
		}
	}
	else {
		abort_ack_remaining[tid] = 0;
		const msg = `[COORD]: ${site} failed to ABORT ${tid}${result === TIMEOUT ? ', timed-out' : ''}`;
		logger.error(msg);
		handleTransactionComplete(tid, result, msg);
	}
}

function handleTransactionComplete(tid, result, reason) {
	const res = client_res[tid];
	if (result === SUCCESS) {
		res.end("SUCCESS");
	}
	else {
		res.end(`FAILED: ${reason}`);
	}
}

function getTransactionState(_tid) {
	const filteredLogs = logs.filter(({ tid }) => tid === _tid);
	if (filteredLogs.length) {
		return filteredLogs[filteredLogs.length - 1].type;
	}
	return null;
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

function canExecute({ failAt }, phase) {
	if (failAt) {
		const coordinatorFailure = failAt['coordinator'];
		return coordinatorFailure && coordinatorFailure.during === phase ? FAILURE : SUCCESS;
	}
	return SUCCESS;
}

function recover(tid) {
	delete transactions[tid].failAt;

	const previousStage = getTransactionState(tid);
	if (previousStage === 'prepare') {
		prepareTransaction(tid);
		process.nextTick(() => askToCommit(tid));
	}
	else if (previousStage === 'commit') {
		commitTransaction(tid);
		process.nextTick(() => handleTransactionComplete(tid, SUCCESS));
	}
}

module.exports = { transact, getTransactionState };