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
const flag = {};

function transact(tid, transactionData, res) {
	client_res[tid] = res;
	transactions[tid] = JSON.parse(transactionData);

	done_remaining[tid] = sites.length;
	flag[tid] = false;
	sites.forEach(site => {
		logger.info(`[COORD]: Sending transaction ${tid} to ${site}`);
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
	done_remaining[tid]--;

	if (result === SUCCESS) {
		logger.info(`[COORD]: Transaction ${tid} received by ${site}`);
	} else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} failed to receive ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	if (done_remaining[tid] === 0) {
		if (flag[tid]) {
			saveTransaction(tid, transactions[tid]);
			logger.warn(`[COORD]: Could not propagate transaction ${tid} to everybody`);
			handleTransactionComplete(tid, FAILURE, "Failed to propagate transaction");
		}
		else {
			saveTransaction(tid, transactions[tid]);
			logger.info(`[COORD]: All sites received ${tid}`);
			process.nextTick(() => askToPrepare(tid));
		}
	}
}

function askToPrepare(tid) {
	logs.push({ type: 'prepare', tid });

	ready_remaining[tid] = sites.length;
	flag[tid] = false;
	sites.forEach(site => {
		logger.info(`[COORD]: Sending <PREPARE ${tid}> to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/prepare/${tid}`), TIMOUT_TIME)
			.then(({ ok }) => handlePrepareResult(resultify(ok), tid, site))
			.catch(({ message }) => handlePrepareResult(resultify(message), tid, site));
	});
}

function handlePrepareResult(result, tid, site) {
	ready_remaining[tid]--;

	if (result === SUCCESS) {
		logger.info(`[COORD]: Received <READY ${tid}> from ${site}`);
	}
	else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} NOT READY ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	if (ready_remaining[tid] === 0) {
		if (flag[tid]) {
			prepareTransaction(tid);
			logger.warn(`[COORD]: Some site/s NOT READY to commit ${tid}`);
			process.nextTick(() => askToAbort(tid));
		} else {
			if (canExecute(transactions[tid], 'after-ready')) {
				prepareTransaction(tid);
				logger.info(`[COORD]: All sites READY to commit ${tid}`);
				process.nextTick(() => askToCommit(tid));
			} else {
				logger.warn(`[COORD]: Failure! ${tid}`);
				process.nextTick(() => recover(tid));
			}
		}
	}
}

function askToCommit(tid) {
	logs.push({ type: 'commit', tid });

	commit_ack_remaining[tid] = sites.length;
	flag[tid] = false;
	sites.forEach(site => {
		logger.info(`[COORD]: Sending <COMMIT ${tid}> to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/commit/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleCommitResult(resultify(ok), tid, site))
			.catch(({ message }) => handleCommitResult(resultify(message), tid, site));
	});
}

function handleCommitResult(result, tid, site) {
	commit_ack_remaining[tid]--;

	if (result === SUCCESS) {
		logger.info(`[COORD]: Received <COMMIT ACK ${tid}> from ${site}`);
	}
	else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} failed to COMMIT ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	if (commit_ack_remaining[tid] === 0) {
		if (flag[tid]) {
			commitTransaction(tid);
			logger.info(`[COORD]: All sites commited ${tid}, site/s had to recover`);
			handleTransactionComplete(tid, SUCCESS, 'site/s had to recover');
		} else {
			if (canExecute(transactions[tid], 'after-commit')) {
				commitTransaction(tid);
				logger.info(`[COORD]: All sites commited ${tid}`);
				handleTransactionComplete(tid, SUCCESS);
			}
			else {
				logger.warn(`[COORD]: Failure! ${tid}`);
				process.nextTick(() => recover(tid));
			}
		}
	}
}

function askToAbort(tid) {
	logs.push({ type: 'abort', tid });

	abort_ack_remaining[tid] = sites.length;
	flag[tid] = false;
	sites.forEach(site => {
		logger.info(`[COORD]: Sending <ABORT ${tid}> to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/abort/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleAbortResult(resultify(ok), tid, site))
			.catch(({ message }) => handleAbortResult(resultify(message), tid, site));
	});
}

function handleAbortResult(result, tid, site) {
	abort_ack_remaining[tid]--;

	if (result === SUCCESS) {
		logger.info(`[COORD]: Received <ABORT ACK ${tid}> from ${site}`);
	}
	else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} failed to ABORT ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	if (abort_ack_remaining[tid] === 0) {
		abortTransaction(tid);
		if (flag[tid]) {
			logger.warn(`[COORD]: Aborted ${tid}, some haven't ACK`);
		} else {
			logger.warn(`[COORD]: All sites aborted ${tid}`);
		}
		handleTransactionComplete(tid, FAILURE, 'some site/s NOT READY');
	}
}

function handleTransactionComplete(tid, result, reason) {
	const res = client_res[tid];
	if (result === SUCCESS) {
		res.end("SUCCESS" + (reason ? `: ${reason}` : ''));
		logger.info(`[COORD]: Transaction ${tid} successful`);
	}
	else {
		res.end("FAILED" + (reason ? `: ${reason}` : ''));
		logger.warn(`[COORD]: Transaction ${tid} failed`);
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
	logger.info(`[COORD]: Attempting to recover ${tid}`);

	delete transactions[tid].failAt;

	const previousStage = getTransactionState(tid);
	if (previousStage === 'prepare') {
		logger.info(`[COORD]: All remote-sites were READY, resuming`);
		prepareTransaction(tid);
		process.nextTick(() => askToCommit(tid));
	}
	else if (previousStage === 'commit') {
		logger.info(`[COORD]: All remote-sites had commited, resuming`);
		commitTransaction(tid);
		handleTransactionComplete(tid, SUCCESS);
	}
}

module.exports = { transact, getTransactionState };