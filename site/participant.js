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

function saveTransaction(tid, transaction) {
	transactions[tid] = transaction;
	logger.info("Transaction received");
	logger.info("Transaction logs: ");
	console.log(transactions);
	console.log();
}

function prepareTransaction(tid) {
	logger.info(`Received <PREPARE T> ${tid}`);
	if (!canExecute(transactions[tid], 'prepare')) {
		process.nextTick(() => recover(tid, 'prepare'));
		return FAILURE;
	}
	const { instructions } = transactions[tid];
	const state = clone(db);
	math.evaluate(instructions, state)
	pendingCommits[tid] = state;

	logger.info("Pending commits:");
	console.log(pendingCommits);
	console.log();
	return SUCCESS;
}

function commitTransaction(tid) {
	logger.info(`Received <COMMIT T> ${tid}`);
	if (!canExecute(transactions[tid], 'commit')) {
		process.nextTick(() => recover(tid, 'commit'));
		return FAILURE;
	}

	logger.info("DB before commit: " + JSON.stringify(db));
	logger.info(`Committing transaction ${tid}`);
	commit = pendingCommits[tid];
	for (let variable in commit) {
		db[variable] = commit[variable];
	}
	delete transactions[tid];
	delete pendingCommits[tid];

	logger.info("DB after commit: " + JSON.stringify(db));
	return SUCCESS;
}

function abortTransaction(tid) {
	logger.info(`Received <ABORT T> ${tid}`);

	delete transactions[tid];
	delete pendingCommits[tid];

	logger.info("Transaction logs:");
	console.log(transactions);
	console.log();
	logger.info("Pending commits:");
	console.log(pendingCommits);
	console.log();
}

function clone(simpleObj) {
	return JSON.parse(JSON.stringify(simpleObj));
}

async function recover(tid, fromPhase) {
	const state = await getTransactionState(tid);
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