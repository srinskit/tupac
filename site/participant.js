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

function saveTransaction(tid, transaction) {
	transactions[tid] = transaction;
	logger.info(`Received transaction ${tid}`);
}

function prepareTransaction(tid) {
	logs.push({ type: 'prepare', tid });

	if (!canExecute(transactions[tid], 'prepare')) {
		logger.warn(`Failed to prepare ${tid}`);
		process.nextTick(() => recover(tid, 'prepare'));
		return FAILURE;
	}
	const { instructions } = transactions[tid];
	const state = clone(db);
	math.evaluate(instructions, state)
	pendingCommits[tid] = state;

	logger.info(`Prepared transaction ${tid}`);
	return SUCCESS;
}

function commitTransaction(tid) {
	logs.push({ type: 'commit', tid });

	if (!canExecute(transactions[tid], 'commit')) {
		logger.warn(`Failed to commit ${tid}`);
		process.nextTick(() => recover(tid, 'commit'));
		return FAILURE;
	}

	logger.info(`Committing transaction ${tid}`);
	logger.info("DB before commit: " + JSON.stringify(db));

	commit = pendingCommits[tid];
	for (let variable in commit) {
		db[variable] = commit[variable];
	}
	delete transactions[tid];
	delete pendingCommits[tid];

	logger.info("DB after  commit: " + JSON.stringify(db));
	return SUCCESS;
}

function abortTransaction(tid) {
	logs.push({ type: 'abort', tid });

	delete transactions[tid];
	delete pendingCommits[tid];

	logger.info(`Aborted transaction ${tid}`);
	logger.info("DB after abort: " + JSON.stringify(db));
}

function clone(simpleObj) {
	return JSON.parse(JSON.stringify(simpleObj));
}

async function recover(tid) {
	logger.info(`Attempting to recover ${tid}`);

	delete transactions[tid].failAt;

	let fromPhase = null;
	const _tid = tid;
	const filteredLogs = logs.filter(({ tid }) => tid === _tid);
	if (filteredLogs.length) {
		fromPhase = filteredLogs[filteredLogs.length - 1].type;
	}

	if (fromPhase === 'prepare') {
		logger.info('Recovery not needed, abort will be issued');
	}
	else if (fromPhase === 'commit') {
		const stateInCoordinator = await getTransactionState(tid);
		if (stateInCoordinator === 'commit') {
			logger.info('Coordinator says commit was issued, recovering by commiting');
			commitTransaction(tid);
		}
	}
}

async function getTransactionState(tid) {
	const r = await fetch(`${coordinator[tid]}/coordinator/tstate/${tid}`);
	if (r.ok) {
		return await r.text();
	}
	return null;
}

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