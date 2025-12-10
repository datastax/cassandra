# VS Code Configuration for Cassandra

This directory contains template files for VS Code configuration that try to match the project's IntelliJ code style.

## Generating VS Code Files

Run the following command to generate/update VS Code configuration:

```bash
ant generate-vscode-files
```

**Note:** After running this command:
- Reload the VS Code window: `Cmd/Ctrl+Shift+P` → "Developer: Reload Window"
- Wait for the Java Language Server to index the project (watch the status bar - this may take a few minutes)

**Troubleshooting:**
If you still see errors after reloading:
1. Check that Java 11 is selected: `Cmd/Ctrl+Shift+P` → "Java: Configure Java Runtime"
2. Clean the Java workspace: `Cmd/Ctrl+Shift+P` → "Java: Clean Java Language Server Workspace"
3. Reload the window again

## Customizing Settings

### Option 1: User Settings (Recommended)
Add your personal preferences to VS Code's User Settings (Cmd/Ctrl+,). These will apply across all projects and won't be overwritten.

Example - Enable format on save for all projects:
```json
{
    "[java]": {
        "editor.formatOnSave": true
    }
}
```

### Option 2: Workspace Settings
Create a `.vscode/settings.local.json` file (not tracked by git) for project-specific overrides that won't be regenerated:

```json
{
    "[java]": {
        "editor.formatOnSave": true,
        "editor.formatOnSaveMode": "file"
    }
}
```

Then add this to your `.vscode/settings.json` (manually, after generation):
```json
{
    // ... existing settings ...

    // Load local overrides (not tracked in git)
    "files.associations": {
        "settings.local.json": "jsonc"
    }
}
```

### Option 3: Modify Template
If you want to change the default for all developers, modify the template files in `ide/vscode/` and commit them.

## Format on Save Options

The default configuration has:
- `"editor.formatOnSave": false` - Manual formatting only (Shift+Alt+F)
- `"editor.formatOnSaveMode": "modificationsIfAvailable"` - When enabled, formats only modified lines

To enable format on save, add to your User Settings:
```json
{
    "[java]": {
        "editor.formatOnSave": true
    }
}
```

## Recommended Extensions

When you open the project, VS Code will suggest installing recommended extensions. Key extensions include:
- Java Extension Pack (Red Hat Java, Debugger, Test Runner, Maven)
- SonarLint (code quality)
- ANTLR4 (grammar file support)
- GitLens (enhanced git integration)
