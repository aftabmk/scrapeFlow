const { Schema } = require ('./models/schema.js');
const { Validator } = require ('./algorithms/Validator.js');

// Define schema once — Ajv compiles it into a native function
const schema    = new Schema({ name: 'string', age: 'number'});
const validator = new Validator(schema);

console.log(validator.validate({ name: 'Aftab', age: 25 }));
console.log(validator.validate({ name: 'Aftab', age: '25' }));
console.log(validator.validate({ name: 'Aftab' }));
console.log(validator.validate({ name: 'Aftab', age: 25, extra: true }));