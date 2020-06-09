const { createLogger, format, transports } = require('winston');

module.exports = createLogger({
	level: 'debug',
	format: format.combine(
		format.timestamp({ format: 'hh:mm:ss' }),
		format.colorize(),
		format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
	),
	transports: [new transports.Console()]
});