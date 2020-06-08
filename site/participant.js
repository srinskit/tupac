const { create, all } = require('mathjs');
const math = create(all, {});
const logger = require('./logger');

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

function clone(simpleObj) {
	return JSON.parse(JSON.stringify(simpleObj));
}

module.exports = { db, saveTransaction, prepareTransaction, commitTransaction, rollbackTransaction };