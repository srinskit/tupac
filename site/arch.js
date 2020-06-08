function getPortOfPeer(name) {
	return {
		'site1': 3000,
		'site2': 4000,
		'site3': 5000,
		'site4': 6000
	}[name];
}

function getUrlOfPeer(name) {
	return `http://localhost:${getPortOfPeer(name)}`;
}

module.exports = { getUrlOfPeer, getPortOfPeer };