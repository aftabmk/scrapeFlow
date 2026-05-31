const LocalEnvironment = require('./localEnvironment');
const LambdaEnvironment = require('./lambdaEnvironment');

class BrowserEnvironment {
  constructor() {
    this.isLambda = !!process.env.AWS_LAMBDA_FUNCTION_NAME;
  }

  getEnvironment() {
    return this.isLambda
      ? new LambdaEnvironment()
      : new LocalEnvironment();
  }

  async launch() {
    const environment = this.getEnvironment();
    return environment.launch();
  }
}

module.exports = BrowserEnvironment;