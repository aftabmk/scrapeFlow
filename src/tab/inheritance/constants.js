// error
const ERROR = Object.freeze({
  NETWORK : 'NetworkError',
  TIMEOUT : 'TimeoutError',
  NAVIGATION : 'NavigationError',
})

// resources
const RESOURCES = Object.freeze({
  XHR:'xhr',
  FONT:'font',
  OTHER:'other',
  MEDIA:'media',
  IMAGE:'image',
  SCRIPT:'script',
  WEBSOCKET:'websocket',
  STYLESHEET : 'stylesheet',
});

const BLOCKED_RESOURCE_TYPES = new Set([
	RESOURCES.XHR, 
	RESOURCES.FONT, 
	RESOURCES.OTHER,
	RESOURCES.MEDIA, 
	RESOURCES.IMAGE, 
	RESOURCES.SCRIPT,
	RESOURCES.WEBSOCKET, 
	RESOURCES.STYLESHEET, 
]);

// wait
const WAIT_UNTIL = Object.freeze({
  LOAD: 'load',
  NETWORK_IDLE_0: 'networkidle0',
  NETWORK_IDLE_2: 'networkidle2',
  DOM_CONTENT_LOADED: 'domcontentloaded',
});

const CONTENT_TYPE = Object.freeze({
  JSON: 'application/json',
  TEXT: 'text/plain',
  HTML: 'text/html',
  XML: 'application/xml',
  FORM_URLENCODED: 'application/x-www-form-urlencoded',
  FORM_DATA: 'multipart/form-data',
  JAVASCRIPT: 'application/javascript',
  PDF: 'application/pdf',
  OCTET_STREAM: 'application/octet-stream',
  PNG: 'image/png',
  JPEG: 'image/jpeg',
  GIF: 'image/gif',
  SVG: 'image/svg+xml',
});

module.exports = { ERROR, BLOCKED_RESOURCE_TYPES, WAIT_UNTIL, CONTENT_TYPE }