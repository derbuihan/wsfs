import * as path from 'path';
import Mocha from 'mocha';

export function run(): Promise<void> {
  const mocha = new Mocha({
    ui: 'tdd',
    color: true,
    timeout: 180000
  });

  const testFile = path.resolve(__dirname, 'core-devloop.test.js');
  mocha.addFile(testFile);

  return new Promise((resolve, reject) => {
    try {
      mocha.run((failures: number) => {
        if (failures && failures > 0) {
          reject(new Error(`${failures} tests failed.`));
        } else {
          resolve();
        }
      });
    } catch (err) {
      reject(err);
    }
  });
}
