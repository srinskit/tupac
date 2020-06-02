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

const db = { a: 0, b: 0, c: 0 };



const port = getPortOfPeer(process.argv[2])
if (!port) {
	logger.error("Invalid site");
	process.exit(1);
}

const express = require('express');
const morgan = require('morgan');
const status = require('http-status');

const app = express();

app.set('port', port);
app.use(morgan('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.post('/transact/:tid', (req, res) => {
	setTimeout(() => res.send("DONE"), randInt(5000));
});
app.get('/query_to_commit/:tid', (req, res) => {
	setTimeout(() => res.send("READY"), randInt(5000));
});
app.post('/commit/:tid', (req, res) => {
	setTimeout(() => res.send("ACK"), randInt(5000));
});
app.post('/rollback/:tid', (req, res) => {
	setTimeout(() => res.send("ACK"), randInt(5000));
});

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

function randInt(max) {
	return Math.floor(Math.random() * max);
}