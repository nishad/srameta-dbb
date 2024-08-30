import Database from "better-sqlite3";
import fs from "fs-extra";
import readline from "readline";
import ora from "ora";
import path from "path";
import { fileURLToPath } from "url";
import { confirm } from "@clack/prompts";

import { downloadSRAAccessionsFile } from "../download/sra-accessions.js";

// Convert import.meta.url to a file path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_FOLDER = path.resolve(__dirname, "../../data");
const DB_FOLDER = path.resolve(__dirname, "../../db");
const ACCESSIONS_FILE = "SRA_Accessions.tab";

export async function buildAccessionsDb() {
  const tsvFilePath = path.join(DATA_FOLDER, ACCESSIONS_FILE);
  const dbPath = path.join(DB_FOLDER, "SRA_Accessions.sqlite");

  // Check if the SRAAccessions.tab file exists
  if (!(await fs.pathExists(tsvFilePath))) {
    console.error(
      `The ${ACCESSIONS_FILE} file does not exist in ${DATA_FOLDER}.`,
    );
    const downloadFile = await confirm({
      message:
        "Would you like to download or generate the SRAAccessions.tab file first?",
    });

    if (!downloadFile) {
      console.log("Process cancelled.");
      return;
    }

    downloadSRAAccessionsFile();
    return;
  }

  // Ensure the db folder exists
  await fs.ensureDir(DB_FOLDER);

  const db = new Database(dbPath);

  // Create the accessions table with snake_case column names
  db.exec(`
    CREATE TABLE IF NOT EXISTS accessions (
      accession TEXT,
      submission TEXT,
      status TEXT,
      updated TEXT,
      published TEXT,
      received TEXT,
      type TEXT,
      center TEXT,
      visibility TEXT,
      alias TEXT,
      experiment TEXT,
      sample TEXT,
      study TEXT,
      loaded INTEGER,
      spots INTEGER,
      bases INTEGER,
      md5sum TEXT,
      biosample TEXT,
      bioproject TEXT,
      replaced_by TEXT
    );
  `);

  // Create indexes on the specified columns
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_accession ON accessions (accession);
    CREATE INDEX IF NOT EXISTS idx_submission ON accessions (submission);
    CREATE INDEX IF NOT EXISTS idx_type ON accessions (type);
    CREATE INDEX IF NOT EXISTS idx_experiment ON accessions (experiment);
    CREATE INDEX IF NOT EXISTS idx_sample ON accessions (sample);
    CREATE INDEX IF NOT EXISTS idx_study ON accessions (study);
    CREATE INDEX IF NOT EXISTS idx_published ON accessions (published);
  `);

  const insert = db.prepare(`
    INSERT INTO accessions (
      accession, submission, status, updated, published, received,
      type, center, visibility, alias, experiment, sample, study,
      loaded, spots, bases, md5sum, biosample, bioproject, replaced_by
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const transaction = db.transaction((rows) => {
    for (const row of rows) {
      insert.run(...row);
    }
  });

  const spinner = ora(`Processing ${ACCESSIONS_FILE}...`).start();

  try {
    const rows = [];
    const fileStream = fs.createReadStream(tsvFilePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });

    let lineCount = 0;
    let isHeader = true;

    for await (const line of rl) {
      const values = line.split("\t");
      if (isHeader) {
        isHeader = false; // Skip the header row
        continue;
      }

      rows.push(values);
      lineCount++;

      if (rows.length >= 1000) {
        // Adjust batch size as needed
        transaction(rows);
        rows.length = 0; // Clear the array after inserting
        spinner.text = `Processed ${lineCount} lines...`;
      }
    }

    // Insert any remaining rows
    if (rows.length > 0) {
      transaction(rows);
    }

    spinner.succeed(
      `${ACCESSIONS_FILE} processing complete. Total lines processed: ${lineCount}`,
    );
  } catch (error) {
    spinner.fail(`Failed to process ${ACCESSIONS_FILE}.`);
    console.error("Error:", error.message);
  } finally {
    db.close();
  }
}

export default buildAccessionsDb;
