const fs = require('fs');

class WriteAheadLog {
  constructor(filePath) {
    this.path = filePath;
    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, '');
    }
  }

  append(entry) {
    fs.appendFileSync(this.path, JSON.stringify(entry) + '\n');
  }

  replay() {
    const content = fs.readFileSync(this.path, 'utf8');
    return content.split('\n').filter(Boolean).map((line) => JSON.parse(line));
  }

  clear() {
    fs.writeFileSync(this.path, "");
  }
}

module.exports = WriteAheadLog;