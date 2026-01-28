# Connect Cursor to Databricks

**Yes.** You can connect **Cursor** to Databricks using the **Databricks extension for Visual Studio Code**. Cursor is VS Code–based, and the extension explicitly supports Cursor.

---

## 1. Install the Databricks extension

1. In Cursor, open **Extensions** (`Ctrl+Shift+X` or **View → Extensions**).
2. Search for **Databricks**.
3. Install the **Databricks** extension (verified by **Databricks**).
4. **Reload** Cursor if prompted.

**Requirements**: Cursor/VS Code 1.86+, Python configured, and a Databricks workspace with at least one **cluster** (not SQL warehouses).

---

## 2. Convert this project to a Databricks project

1. **File → Open Folder** and open `fmucd-facility-maintenance-analytics`.
2. Click the **Databricks** icon in the left sidebar.
3. Click **Create configuration** (or **Convert project**).
4. In the Command Palette:
   - **Select authentication method**: choose **OAuth (user to machine)** (recommended) or **Personal Access Token**.
   - If OAuth: name the profile → **Login to Databricks** → complete browser sign-in → allow **all-apis**.
   - If PAT: name the profile → enter workspace **host** (e.g. `https://<workspace>.cloud.databricks.com`) → create a [PAT](https://docs.databricks.com/en/dev-tools/auth/pat.html) in the workspace → paste the token.

The extension creates `.databricks/` (and optionally `databricks.yml`) in your project. Add `.databricks/` to `.gitignore` if it isn’t already (it contains auth/config).

---

## 3. Configure compute and auth

In the Databricks extension **Configuration** view:

1. **Auth Type** → gear icon → pick your profile (or sign in).
2. **Cluster** → **Select a cluster** or **Configure cluster**:
   - **Serverless** (Databricks-managed), or  
   - **Use an existing cluster** (Unity Catalog–enabled for the capstone), or  
   - **Create New Cluster** (opens workspace; create a cluster, then select it).
3. **Python Environment** → **Activate Virtual Environment** → choose Venv or Conda, install deps if prompted.  
   Use **Databricks Connect** for local debugging; the extension can guide setup.

---

## 4. What you can do from Cursor

- **Run Python files on a cluster**: Open a `.py` file (e.g. `notebooks/01_bronze_ingest.py`) → right‑click → **Run File on Databricks** (or use the Run button).
- **Run as a job**: **Run as Databricks Job** to execute the file as a Lakeflow Job.
- **Notebooks**: The extension can run `.ipynb` and other notebooks as jobs; for cell‑by‑cell debugging, use **Databricks Connect** with the extension.
- **Workspace sync**: Use **Remote Folder** sync to push local changes to a workspace folder (one‑way, local → remote). For Repos-based workflows, use **Databricks Repos** in the workspace instead.

---

## 5. Capstone-specific notes

- **Paths**: Update `FMUCD_CSV_PATH` in `01_bronze_ingest` to a path the **Databricks** cluster can read (e.g. `/Workspace/Repos/.../docs/...FMUCD.csv`, or DBFS/cloud storage).
- **Unity Catalog**: Use a cluster with Unity Catalog enabled so `CREATE CATALOG` / `CREATE SCHEMA` and table creation work.
- **Orchestration**: You can develop and run individual notebooks from Cursor; for the full pipeline, use a **Databricks Job** as in [WORKFLOW_JOB.md](WORKFLOW_JOB.md) (created in the workspace UI or via API).

---

## 6. Official links

- [Databricks extension for VS Code (works with Cursor)](https://docs.databricks.com/en/dev-tools/vscode-ext/)
- [Install](https://docs.databricks.com/en/dev-tools/vscode-ext/install.html)
- [Configure](https://docs.databricks.com/en/dev-tools/vscode-ext/configure.html)
- [Authentication](https://docs.databricks.com/en/dev-tools/vscode-ext/authentication.html)
- [Debug with Databricks Connect](https://docs.databricks.com/en/dev-tools/vscode-ext/databricks-connect.html)
