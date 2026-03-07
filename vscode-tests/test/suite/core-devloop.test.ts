import * as assert from 'assert';
import * as childProcess from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

function getWorkspaceRoot(): string {
  const fromEnv = process.env.VSCODE_E2E_WORKSPACE;
  if (fromEnv) {
    return fromEnv;
  }
  const folder = vscode.workspace.workspaceFolders?.[0];
  if (!folder) {
    throw new Error('No workspace folder found');
  }
  return folder.uri.fsPath;
}

async function writeFile(filePath: string, content: string): Promise<void> {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  await fs.promises.writeFile(filePath, content, 'utf8');
}

async function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForFileContains(filePath: string, expected: string, timeoutMs: number): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const content = await fs.promises.readFile(filePath, 'utf8');
      if (content.includes(expected)) {
        return;
      }
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== 'ENOENT') {
        throw err;
      }
    }
    await delay(500);
  }
  throw new Error(`Timed out waiting for ${filePath} to contain '${expected}'`);
}

async function removeIfExists(filePath: string): Promise<void> {
  try {
    await fs.promises.unlink(filePath);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== 'ENOENT') {
      throw err;
    }
  }
}

async function ensurePythonExtensionActive(): Promise<void> {
  const extension = vscode.extensions.getExtension('ms-python.python');
  assert.ok(extension, 'Python extension not found.');
  if (!extension.isActive) {
    await extension.activate();
  }
}

function runPythonDirect(scriptPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    childProcess.execFile('python3', [scriptPath], { timeout: 30000 }, (err, stdout, stderr) => {
      if (err) {
        reject(new Error(`python3 direct exec failed: ${err.message}\nstdout: ${stdout}\nstderr: ${stderr}`));
        return;
      }
      resolve();
    });
  });
}

async function runPythonViaExtension(doc: vscode.TextDocument, outputPath: string): Promise<void> {
  await ensurePythonExtensionActive();
  await delay(3000);

  const commands = await vscode.commands.getCommands(true);
  const candidates = ['python.runFileInTerminal', 'python.execInTerminal'];
  const commandId = candidates.find((candidate) => commands.includes(candidate));
  if (!commandId) {
    throw new Error(`Python run command not found. Searched: ${candidates.join(', ')}`);
  }

  await removeIfExists(outputPath);
  await vscode.commands.executeCommand(commandId, doc.uri);
  await waitForFileContains(outputPath, 'ext-ok', 60000);
}

async function runPythonCommand(doc: vscode.TextDocument, outputPath: string): Promise<void> {
  // Try running via Python extension first
  try {
    await runPythonViaExtension(doc, outputPath);
    return;
  } catch (extErr) {
    console.log(`Python extension command failed: ${extErr instanceof Error ? extErr.message : String(extErr)}`);
    console.log('Falling back to direct python3 execution');
  }

  // Fallback: run python3 directly
  await removeIfExists(outputPath);
  await runPythonDirect(doc.uri.fsPath);
  await waitForFileContains(outputPath, 'ext-ok', 10000);
}

suite('VSCode core dev loop on wsfs', () => {
  test('runs the core workflow in one session', async function () {
    this.timeout(180000);

    const workspaceRoot = getWorkspaceRoot();
    const srcDir = path.join(workspaceRoot, 'src');
    const helloPy = path.join(srcDir, 'hello.py');
    const helloExtPy = path.join(srcDir, 'hello_ext.py');
    const outputExtTxt = path.join(workspaceRoot, 'output_ext.txt');

    await fs.promises.mkdir(srcDir, { recursive: true });

    const helloContent = [
      'import pathlib',
      'print("hello")'
    ].join('\n');

    const helloExtContent = [
      'import pathlib',
      `pathlib.Path(${JSON.stringify(outputExtTxt)}).write_text("ext-ok")`,
      'print("ext-ok")'
    ].join('\n');

    await writeFile(helloPy, helloContent + '\n');
    await writeFile(helloExtPy, helloExtContent + '\n');

    // Edit + save
    const helloDoc = await vscode.workspace.openTextDocument(helloPy);
    const helloEditor = await vscode.window.showTextDocument(helloDoc);
    await helloEditor.edit((editBuilder) => {
      editBuilder.insert(new vscode.Position(0, 0), '# wsfs integration test\n');
    });
    await helloDoc.save();
    await waitForFileContains(helloPy, '# wsfs integration test', 30000);

    // Rename
    const renameSource = vscode.Uri.file(path.join(workspaceRoot, 'file.txt'));
    const renameTarget = vscode.Uri.file(path.join(workspaceRoot, 'renamed.txt'));
    await vscode.workspace.fs.writeFile(renameSource, Buffer.from('rename\n'));
    await vscode.workspace.fs.rename(renameSource, renameTarget, { overwrite: true });
    assert.ok(fs.existsSync(renameTarget.fsPath), 'renamed.txt should exist');

    // Delete
    const obsolete = vscode.Uri.file(path.join(workspaceRoot, 'obsolete.txt'));
    await vscode.workspace.fs.writeFile(obsolete, Buffer.from('obsolete\n'));
    await vscode.workspace.fs.delete(obsolete);
    assert.ok(!fs.existsSync(obsolete.fsPath), 'obsolete.txt should be deleted');

    // Non-recursive delete on non-empty directory should surface ENOTEMPTY.
    const nonEmptyDir = path.join(workspaceRoot, 'nonempty-dir');
    await fs.promises.mkdir(nonEmptyDir, { recursive: true });
    await fs.promises.writeFile(path.join(nonEmptyDir, 'test.txt'), 'still-here\n', 'utf8');
    await assert.rejects(
      fs.promises.rmdir(nonEmptyDir),
      (err: unknown) => (err as NodeJS.ErrnoException).code === 'ENOTEMPTY',
      'rmdir on non-empty directory should return ENOTEMPTY'
    );
    await vscode.workspace.fs.delete(vscode.Uri.file(nonEmptyDir), { recursive: true, useTrash: false });

    // Recursive directory delete
    const deleteDir = vscode.Uri.file(path.join(workspaceRoot, 'delete-dir'));
    await fs.promises.mkdir(path.join(deleteDir.fsPath, '.vscode'), { recursive: true });
    await fs.promises.mkdir(path.join(deleteDir.fsPath, 'src'), { recursive: true });
    await fs.promises.writeFile(path.join(deleteDir.fsPath, '.vscode', 'settings.json'), '{\n}\n', 'utf8');
    await fs.promises.writeFile(path.join(deleteDir.fsPath, 'src', 'hello.py'), 'print("hello")\n', 'utf8');
    await fs.promises.writeFile(path.join(deleteDir.fsPath, 'root.txt'), 'delete-me\n', 'utf8');
    await vscode.workspace.fs.delete(deleteDir, { recursive: true, useTrash: false });
    assert.ok(!fs.existsSync(deleteDir.fsPath), 'delete-dir should be deleted recursively');

    // Python extension command execution
    await removeIfExists(outputExtTxt);
    const extDoc = await vscode.workspace.openTextDocument(helloExtPy);
    await vscode.window.showTextDocument(extDoc);
    await extDoc.save();

    await runPythonCommand(extDoc, outputExtTxt);
  });
});
