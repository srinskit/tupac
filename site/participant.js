const { getSiteName } = require('./arch');
const { create, all } = require('mathjs');
const fetch = require('node-fetch');
const math = create(all, {});
const logger = require('./logger');
const FAILURE = 0, SUCCESS = 1;

const db = {};
const transactions = {};
const pendingCommits = {};
const coordinator = {};
const logs = [];

// This function handles initial save call
function saveTransaction(tid, transaction) {
	// Save transaction so that it can be referenced using id later
	transactions[tid] = transaction;
	logger.info(`Received transaction ${tid}`);
}

// This function handles <PREPARE T> call
function prepareTransaction(tid) {
	// Push the action into the log
	logs.push({ type: 'prepare', tid });

	// Check if transaction can be executed, if not recover
	if (!canExecute(transactions[tid], 'prepare')) {
		logger.warn(`Failed to prepare ${tid}`);
		process.nextTick(() => recover(tid));
		return FAILURE;
	}

	// Process the query and prepare a new DB state
	const { instructions } = transactions[tid];
	const state = clone(db);
	math.evaluate(instructions, state);

	// Save the new DB state seperately, do not apply yet
	pendingCommits[tid] = state;

	logger.info(`Prepared transaction ${tid}`);
	return SUCCESS;
}

// This function handles <COMMIT T> call
function commitTransaction(tid) {
	// Push the action into the log
	logs.push({ type: 'commit', tid });

	// Check if action can be done, if not recover
	if (!canExecute(transactions[tid], 'commit')) {
		logger.warn(`Failed to commit ${tid}`);
		process.nextTick(() => recover(tid));
		return FAILURE;
	}

	logger.info(`Committing transaction ${tid}`);
	logger.info("DB before commit: " + JSON.stringify(db));

	//  Get the new DB state prepared during the <PREPARE T> call
	commit = pendingCommits[tid];
	// Apply the new DB state to the actual DB
	for (let variable in commit) {
		db[variable] = commit[variable];
	}
	// Cleanup
	delete transactions[tid];
	delete pendingCommits[tid];

	logger.info("DB after  commit: " + JSON.stringify(db));
	return SUCCESS;
}

// This function handles <ABORT T> call
function abortTransaction(tid) {
	// Push the action into the log
	logs.push({ type: 'abort', tid });

	//  Discard the new DB state created during <PREPARE T> call
	// Cleanup
	delete pendingCommits[tid];
	delete transactions[tid];

	logger.info(`Aborted transaction ${tid}`);
	logger.info("DB after abort: " + JSON.stringify(db));
}

function clone(simpleObj) {
	return JSON.parse(JSON.stringify(simpleObj));
}

// Recover from a failure at participant
async function recover(tid) {
	logger.info(`Attempting to recover ${tid}`);

	// Remove cause of failure (simulated)
	delete transactions[tid].failAt;

	let fromPhase = null;
	const _tid = tid;
	// Filter through logs to find logs relavent to tid
	const filteredLogs = logs.filter(({ tid }) => tid === _tid);
	if (filteredLogs.length) {
		// Get the last action performed for that transaction
		fromPhase = filteredLogs[filteredLogs.length - 1].type;
	}

	// If failed during PREPARE, we don't have to explictly recover
	if (fromPhase === 'prepare') {
		// Coordinator will give instructions
		logger.info('Recovery not needed, abort will be issued');
	}
	// If failed during COMMIT
	else if (fromPhase === 'commit') {
		// Query coordinator to see status of transaction
		const stateInCoordinator = await getTransactionState(tid);
		// If coordinator has commited, perform commit here also
		if (stateInCoordinator === 'commit') {
			logger.info('Coordinator says commit was issued, recovering by commiting');
			commitTransaction(tid);
		}
	}
}

// Get the status of a transaction from the coordinator
async function getTransactionState(tid) {
	// HTTP GET request
	const r = await fetch(`${coordinator[tid]}/coordinator/tstate/${tid}`);
	if (r.ok) {
		return await r.text();
	}
	return null;
}

// To simulate failure, the site and phase of failure is given 
// in the transaction. This function helps identify it.
function canExecute({ failAt }, phase) {
	if (failAt) {
		const siteFailure = failAt[getSiteName()];
		return siteFailure && siteFailure.during === phase ? FAILURE : SUCCESS;
	}
	return SUCCESS;
}

function setCoordinator(tid, url) {
	coordinator[tid] = url;
}

module.exports = { db, saveTransaction, prepareTransaction, commitTransaction, abortTransaction, setCoordinator };