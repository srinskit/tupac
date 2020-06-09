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

// Handle transaction sent to the coordinator by client
function transact(tid, transactionData, res) {
	client_res[tid] = res;
	transactions[tid] = JSON.parse(transactionData);

	done_remaining[tid] = sites.length;
	flag[tid] = false;
	// For each participant, make a HTTP POST request with a timeout
	// Send transaction data and id
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

// This function is called for every participant reply from the above function
function handleTransactResult(result, tid, site) {
	done_remaining[tid]--;

	// Successful
	if (result === SUCCESS) {
		logger.info(`[COORD]: Transaction ${tid} received by ${site}`);
	}
	// Rejected or request timed-out
	else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} failed to receive ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	// When the last participant replies, this if is entered
	if (done_remaining[tid] === 0) {
		// If any participant replied with a negative
		if (flag[tid]) {
			// Perform save action in the local-site (for cleaner logging)
			saveTransaction(tid, transactions[tid]);
			logger.warn(`[COORD]: Could not propagate transaction ${tid} to everybody`);
			// Transaction failed, notify client
			handleTransactionComplete(tid, FAILURE, "Failed to propagate transaction");
		}
		// If all is well, procced to make <PREPARE T> call
		else {
			// Perform save action in the local-site
			saveTransaction(tid, transactions[tid]);
			logger.info(`[COORD]: All sites received ${tid}`);
			process.nextTick(() => askToPrepare(tid));
		}
	}
}

// Send <PREPARE T> calls to all participants
function askToPrepare(tid) {
	// Push action to log
	logs.push({ type: 'prepare', tid });

	ready_remaining[tid] = sites.length;
	flag[tid] = false;
	// For each participant, make a HTTP GET request with a timeout
	// Send id
	sites.forEach(site => {
		logger.info(`[COORD]: Sending <PREPARE ${tid}> to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/prepare/${tid}`), TIMOUT_TIME)
			.then(({ ok }) => handlePrepareResult(resultify(ok), tid, site))
			.catch(({ message }) => handlePrepareResult(resultify(message), tid, site));
	});
}

// This function is called for every participant reply from the above function
function handlePrepareResult(result, tid, site) {
	ready_remaining[tid]--;

	// <READY T> is received
	if (result === SUCCESS) {
		logger.info(`[COORD]: Received <READY ${tid}> from ${site}`);
	}
	// <NOT READY T>, error or timeout
	else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} NOT READY ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	// When the last participant replies, this if is entered
	if (ready_remaining[tid] === 0) {
		// If any participant replied with a negative
		if (flag[tid]) {
			// Perform <PREPARE T> in the local-site (for cleaner logging)
			prepareTransaction(tid);
			logger.warn(`[COORD]: Some site/s NOT READY to commit ${tid}`);
			// Proceed to sending <ABORT T> to all participants
			process.nextTick(() => askToAbort(tid));
		} else {
			// Check if coordinator fails at this stage
			if (canExecute(transactions[tid], 'after-ready')) {
				// Perform <PREPARE T> on local-site
				prepareTransaction(tid);
				logger.info(`[COORD]: All sites READY to commit ${tid}`);
				// Proceed to sending <COMMIT T> to all participants
				process.nextTick(() => askToCommit(tid));
			} else {
				// On failure, attempt to recover
				logger.warn(`[COORD]: Failure! ${tid}`);
				process.nextTick(() => recover(tid));
			}
		}
	}
}

// Send <COMMIT T> calls to all participants
function askToCommit(tid) {
	// Push action to log
	logs.push({ type: 'commit', tid });

	commit_ack_remaining[tid] = sites.length;
	flag[tid] = false;
	// For each participant, make a HTTP POST request with a timeout
	// Send id
	sites.forEach(site => {
		logger.info(`[COORD]: Sending <COMMIT ${tid}> to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/commit/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleCommitResult(resultify(ok), tid, site))
			.catch(({ message }) => handleCommitResult(resultify(message), tid, site));
	});
}

// This function is called for every participant reply from the above function
function handleCommitResult(result, tid, site) {
	commit_ack_remaining[tid]--;

	// <COMMIT ACK T> is received
	if (result === SUCCESS) {
		logger.info(`[COORD]: Received <COMMIT ACK ${tid}> from ${site}`);
	}
	// <COMMIT ACK T> is not received i.e., failure or timeout
	else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} failed to COMMIT ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	// When the last participant replies, this if is entered
	if (commit_ack_remaining[tid] === 0) {
		// If any participant replied with a negative
		if (flag[tid]) {
			// Perform <COMMIT T> in the local-site (for cleaner logging)
			commitTransaction(tid);
			logger.info(`[COORD]: All sites commited ${tid}, site/s had to recover`);
			// Transaction complete, as participants can recover, notify client
			handleTransactionComplete(tid, SUCCESS, 'site/s had to recover');
		} else {
			// Check if coordinator fails at this stage
			if (canExecute(transactions[tid], 'after-commit')) {
				// Perform <COMMIT T> in the local-site 
				commitTransaction(tid);
				logger.info(`[COORD]: All sites commited ${tid}`);
				// Transaction complete, notify client
				handleTransactionComplete(tid, SUCCESS);
			}
			else {
				// On failure, attempt to recover
				logger.warn(`[COORD]: Failure! ${tid}`);
				process.nextTick(() => recover(tid));
			}
		}
	}
}

// Send <ABORT T> calls to all participants
function askToAbort(tid) {
	// Push action to log
	logs.push({ type: 'abort', tid });

	abort_ack_remaining[tid] = sites.length;
	flag[tid] = false;
	// For each participant, make a HTTP POST request with a timeout
	// Send id
	sites.forEach(site => {
		logger.info(`[COORD]: Sending <ABORT ${tid}> to ${site}`);
		timeout(fetch(`${getUrlOfPeer(site)}/abort/${tid}`, { method: 'POST' }), TIMOUT_TIME)
			.then(({ ok }) => handleAbortResult(resultify(ok), tid, site))
			.catch(({ message }) => handleAbortResult(resultify(message), tid, site));
	});
}

// This function is called for every participant reply from the above function
function handleAbortResult(result, tid, site) {
	abort_ack_remaining[tid]--;

	// <ABORT ACK T> is received
	if (result === SUCCESS) {
		logger.info(`[COORD]: Received <ABORT ACK ${tid}> from ${site}`);
	}
	// <ABORT ACK T> is not received i.e., failure or timeout
	else {
		flag[tid] = true;
		logger.warn(`[COORD]: ${site} failed to ABORT ${tid}${result === TIMEOUT ? ', timed-out' : ''}`);
	}

	// When the last participant replies, this if is entered
	if (abort_ack_remaining[tid] === 0) {
		// Perform <ABORT T> in the local-site 
		abortTransaction(tid);
		if (flag[tid]) {
			logger.warn(`[COORD]: Aborted ${tid}, some haven't ACK`);
		} else {
			logger.warn(`[COORD]: All sites aborted ${tid}`);
		}
		// Transaction failed, notify client
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

// Get transaction status from coordinator logs
function getTransactionState(_tid) {
	// Filter logs relavent to given transaction
	const filteredLogs = logs.filter(({ tid }) => tid === _tid);
	if (filteredLogs.length) {
		// Get the last attempted action
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

// To simulate failure, the site and phase of failure is given 
// in the transaction. This function helps identify it.
function canExecute({ failAt }, phase) {
	if (failAt) {
		const coordinatorFailure = failAt['coordinator'];
		return coordinatorFailure && coordinatorFailure.during === phase ? FAILURE : SUCCESS;
	}
	return SUCCESS;
}

// Recover from a failure at coordinator
function recover(tid) {
	logger.info(`[COORD]: Attempting to recover ${tid}`);

	// Remove cause of failure (simulated)
	delete transactions[tid].failAt;

	// Get last attempted action
	const previousStage = getTransactionState(tid);
	// Coordinator failed after all participants READY
	if (previousStage === 'prepare') {
		logger.info(`[COORD]: All remote-sites were READY, resuming`);
		// Perform <PREPARE T> on local-site
		prepareTransaction(tid);
		// Resume by sending <COMMIT T> to participants
		process.nextTick(() => askToCommit(tid));
	}
	// Coordinator failed after all participants COMMITED
	else if (previousStage === 'commit') {
		logger.info(`[COORD]: All remote-sites had commited, resuming`);
		// Perform <COMMIT T> on local-site
		commitTransaction(tid);
		// Transaction complete, notify client
		handleTransactionComplete(tid, SUCCESS);
	}
}

module.exports = { transact, getTransactionState };