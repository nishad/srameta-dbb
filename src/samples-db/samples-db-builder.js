import Database from "better-sqlite3";
import { Worker } from "worker_threads";
import ora from "ora";
import fs from "fs-extra";
import path from "path";
import { fileURLToPath } from "url";
import { confirm } from "@clack/prompts";

import { buildXmlDb } from "../xml-db/xml-db-builder.js";

// Convert import.meta.url to a file path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DB_FOLDER = path.resolve(__dirname, "../../db");
const SOURCE_DB_FILE = "SRAmetadb_XML.sqlite";

export async function buildSamplesDb(
  targetDbName = "SRAmetadb_samples.sqlite",
  numThreads = 2,
  batchSize = 10,
  pageSize = 10,
) {
  const sourceDbPath = path.join(DB_FOLDER, SOURCE_DB_FILE);
  const targetDbPath = path.join(DB_FOLDER, targetDbName);

  // Check if the source database exists
  if (!(await fs.pathExists(sourceDbPath))) {
    console.error(
      `The source database ${SOURCE_DB_FILE} does not exist in ${DB_FOLDER}.`,
    );
    const generateDb = await confirm({
      message: "Would you like to generate the XML database first?",
    });

    if (!generateDb) {
      console.log("Process cancelled.");
      return;
    }

    // Here you would call the function to generate the XML database
    buildXmlDb();
    return;
  }

  try {
    await processPipeline(
      sourceDbPath,
      targetDbPath,
      numThreads,
      batchSize,
      pageSize,
    );
  } catch (error) {
    console.error("Error during pipeline execution:", error);
  }
}

async function processPipeline(
  sourceDbPath,
  targetDbPath,
  numThreads,
  batchSize,
  pageSize,
) {
  const sourceDb = new Database(sourceDbPath);
  const targetDb = new Database(targetDbPath);
  let batch = [];
  let processedCount = 0;
  let isShuttingDown = false;

  const spinner = ora("Processing database entries...").start();

  targetDb.exec("PRAGMA journal_mode = WAL;");
  targetDb.exec(`
    CREATE TABLE IF NOT EXISTS samples (
      sra_id TEXT PRIMARY KEY,
      type TEXT,
      taxon_ids TEXT,
      json TEXT
    );
  `);

  // Create an index for sra_id
  targetDb.exec(`
    CREATE INDEX IF NOT EXISTS idx_sra_id ON samples (sra_id);
  `);

  process.on("SIGINT", () => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    spinner.warn("SIGINT received. Cleaning up...");

    if (batch.length > 0) {
      insertBatch(targetDb, batch);
    }

    sourceDb.close();
    targetDb.close();
    spinner.succeed("Cleanup complete. Exiting.");
    process.exit(0);
  });

  try {
    for (const rows of paginateQuery(
      sourceDb,
      "SELECT sra_id, type, xml FROM data WHERE type = 'sample'",
      pageSize,
    )) {
      if (isShuttingDown) break;

      for (const record of rows) {
        if (isShuttingDown) break;

        // Check if the sra_id already exists in the samples table
        const recordExists = targetDb
          .prepare("SELECT 1 FROM samples WHERE sra_id = ?")
          .get(record.sra_id);
        if (recordExists) {
          continue;
        }

        try {
          await processRecord(record, targetDb, batch, batchSize, spinner);
          processedCount += 1;

          // Update spinner with minimal progress information
          if (processedCount % batchSize === 0) {
            spinner.text = `Processed ${processedCount} records...`;
          }
        } catch (error) {
          console.error(
            `Failed to process record ${record.sra_id}: ${error.message}`,
          );
          // Continue processing the next record
        }
      }
    }

    if (batch.length > 0) {
      insertBatch(targetDb, batch);
      processedCount += batch.length;
    }

    spinner.succeed(
      `Pipeline completed. Total processed: ${processedCount} records.`,
    );
  } catch (error) {
    console.error("Error during pipeline execution:", error);
  } finally {
    sourceDb.close();
    targetDb.close();
  }
}

function* paginateQuery(db, query, pageSize) {
  let offset = 0;
  let rows;
  do {
    rows = db.prepare(`${query} LIMIT ${pageSize} OFFSET ${offset}`).all();
    yield rows;
    offset += pageSize;
  } while (rows.length > 0);
}

function processRecord(record, targetDb, batch, batchSize, spinner) {
  return new Promise((resolve, reject) => {
    const worker = new Worker("./xmlWorker.js", {
      workerData: { xml: record.xml },
    });

    const timeout = setTimeout(() => {
      worker.terminate();
      reject(new Error(`Worker timed out for record: ${record.sra_id}`));
    }, 10000); // 10 second timeout

    worker.on("message", (message) => {
      clearTimeout(timeout);
      if (message.success) {
        batch.push({
          sra_id: record.sra_id,
          type: record.type,
          taxon_ids: JSON.stringify(message.taxonIds),
          json: JSON.stringify(message.jsonData),
        });

        if (batch.length >= batchSize) {
          insertBatch(targetDb, batch);
          batch.length = 0; // Clear the batch
        }
        resolve();
      } else {
        reject(
          new Error(
            `Error processing record ${record.sra_id}: ${message.error}`,
          ),
        );
      }
    });

    worker.on("error", (err) => {
      clearTimeout(timeout);
      reject(err);
    });

    worker.on("exit", (code) => {
      clearTimeout(timeout);
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}

function insertBatch(db, batch) {
  const insert = db.prepare(
    "INSERT OR IGNORE INTO samples (sra_id, type, taxon_ids, json) VALUES (?, ?, ?, ?)",
  );
  const transaction = db.transaction((batch) => {
    for (const record of batch) {
      insert.run(record.sra_id, record.type, record.taxon_ids, record.json);
    }
  });
  transaction(batch);
}

export default buildSamplesDb;
