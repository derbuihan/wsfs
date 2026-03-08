import * as path from 'path';
import * as childProcess from 'child_process';
import {
  downloadAndUnzipVSCode,
  resolveCliPathFromVSCodeExecutablePath,
  runTests
} from '@vscode/test-electron';

async function installPythonExtension(cliPath: string, extensionsDir: string, userDataDir: string): Promise<void> {
  const result = childProcess.spawnSync(
    cliPath,
    [
      '--install-extension',
      'ms-python.python',
      '--force',
      '--extensions-dir',
      extensionsDir,
      '--user-data-dir',
      userDataDir
    ],
    { stdio: 'inherit' }
  );

  if (result.status !== 0) {
    throw new Error('Failed to install ms-python.python extension');
  }
}

async function main(): Promise<void> {
  const workspace = process.env.VSCODE_E2E_WORKSPACE;
  if (!workspace) {
    throw new Error('VSCODE_E2E_WORKSPACE is required');
  }

  const userDataDir = process.env.VSCODE_TEST_USER_DIR || '/tmp/vscode-test/user-data';
  const extensionsDir = process.env.VSCODE_TEST_EXT_DIR || '/tmp/vscode-test/extensions';

  const vscodeExecutablePath = await downloadAndUnzipVSCode('stable');
  const cliPath = resolveCliPathFromVSCodeExecutablePath(vscodeExecutablePath);

  await installPythonExtension(cliPath, extensionsDir, userDataDir);

  const extensionDevelopmentPath = path.resolve(__dirname, '..', '..', 'extension');
  const extensionTestsPath = path.resolve(__dirname, '..', 'test', 'suite', 'index');

  await runTests({
    vscodeExecutablePath,
    extensionDevelopmentPath,
    extensionTestsPath,
    launchArgs: [
      workspace,
      '--disable-gpu',
      '--no-sandbox',
      '--user-data-dir',
      userDataDir,
      '--extensions-dir',
      extensionsDir
    ],
    extensionTestsEnv: {
      ...process.env,
      VSCODE_E2E_WORKSPACE: workspace
    }
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
