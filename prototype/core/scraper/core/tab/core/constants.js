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
  FETCH :'fetch',
  SCRIPT:'script',
  DOCUMENT :'document',
  WEBSOCKET:'websocket',
  STYLESHEET :'stylesheet',
});

const ALLOWED_RESOURCE_TYPES = new Set([
  RESOURCES.FETCH,
  RESOURCES.DOCUMENT,
]);

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
  GIF: 'image/gif',
  PNG: 'image/png',
  HTML: 'text/html',
  JPEG: 'image/jpeg',
  TEXT: 'text/plain',
  SVG: 'image/svg+xml',
  XML: 'application/xml',
  PDF: 'application/pdf',
  JSON: 'application/json',
  FORM_DATA: 'multipart/form-data',
  JAVASCRIPT: 'application/javascript',
  OCTET_STREAM: 'application/octet-stream',
  FORM_URLENCODED: 'application/x-www-form-urlencoded',
});

module.exports = { ERROR, BLOCKED_RESOURCE_TYPES, ALLOWED_RESOURCE_TYPES , WAIT_UNTIL, CONTENT_TYPE }