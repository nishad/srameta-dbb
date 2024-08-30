import { extract } from "tar-stream";
import gunzip from "gunzip-maybe";
import fs from "fs-extra";
import path from "path";
import Database from "better-sqlite3";
import PQueue from "p-queue";
import ora from "ora";
import { fileURLToPath } from "url";
import { select, confirm } from "@clack/prompts";

// Convert import.meta.url to a file path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_FOLDER = path.resolve(__dirname, "../../data");
const DB_FOLDER = path.resolve(__dirname, "../../db");

export async function buildXmlDb() {
  try {
    // Ensure the db folder exists
    await fs.ensureDir(DB_FOLDER);

    // Check if there are any tar.gz files in the input directory
    const tarFiles = (await fs.readdir(DATA_FOLDER)).filter((file) =>
      file.endsWith(".tar.gz"),
    );

    if (tarFiles.length === 0) {
      console.error(
        "No tar.gz files found in the input directory. Please download the latest SRA metadata archive first.",
      );
      return;
    }

    let archivePath;
    if (tarFiles.length === 1) {
      archivePath = path.join(DATA_FOLDER, tarFiles[0]);
    } else {
      const selectedFile = await select({
        message: "Multiple tar.gz files found. Please select one:",
        options: tarFiles.map((file) => ({ label: file, value: file })),
      });

      if (!selectedFile) {
        console.log("Process cancelled.");
        return;
      }

      archivePath = path.join(DATA_FOLDER, selectedFile);
    }

    const dbPath = path.join(DB_FOLDER, "SRAmetadb_XML.sqlite");

    await processArchive(archivePath, dbPath);
  } catch (error) {
    console.error("Error during pipeline execution:", error);
  }
}

async function processArchive(
  archivePath,
  dbPath,
  numThreads = 4,
  batchSize = 2000,
) {
  const db = new Database(dbPath);
  const writeQueue = new PQueue({ concurrency: 1 }); // Queue to handle single-threaded writes
  let batch = [];
  let processedCount = 0;
  let skippedCount = 0;

  const spinner = ora("Processing files...").start();

  // Initialize the database and create necessary tables
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA synchronous = normal;");
  db.exec("pragma journal_size_limit = 6144000;");

  db.exec(`
    CREATE TABLE IF NOT EXISTS data (
      sra_id TEXT,
      type TEXT,
      xml TEXT,
      PRIMARY KEY (sra_id, type)
    );
  `);

  const extractStream = extract();

  extractStream.on("entry", (header, stream, next) => {
    if (header.type === "file" && path.extname(header.name) === ".xml") {
      const filePathParts = header.name.split("/");
      const sra_id = filePathParts[0];
      const type = filePathParts[1].split(".")[1];

      // Check if this file has already been processed
      const recordExists = db
        .prepare("SELECT 1 FROM data WHERE sra_id = ? AND type = ?")
        .get(sra_id, type);
      if (recordExists) {
        skippedCount++;
        spinner.text = `Skipped ${skippedCount} files. Total processed: ${processedCount} XML files.`;
        stream.resume(); // Ensure the stream is resumed
        next(); // Skip this file and move to the next one
        return;
      }

      let xmlContent = "";

      stream.on("data", (chunk) => {
        xmlContent += chunk.toString();
      });

      stream.on("end", async () => {
        batch.push({ sra_id, type, xml: xmlContent });

        if (batch.length >= batchSize) {
          await writeQueue.add(() => insertBatch(db, batch));
          processedCount += batch.length;
          spinner.text = `Inserted batch of ${batch.length} records. Total processed: ${processedCount} XML files. Skipped: ${skippedCount} files.`;
          batch = []; // Reset the batch
        }
        stream.resume(); // Ensure the stream is resumed after processing
        next();
      });
    } else {
      stream.resume(); // Ensure the stream is resumed for non-XML files
      next();
    }
  });

  extractStream.on("finish", async () => {
    // Write the remaining records if the batch isn't empty
    if (batch.length > 0) {
      await writeQueue.add(() => insertBatch(db, batch));
      processedCount += batch.length;
      spinner.text = `Inserted final batch of ${batch.length} records. Total processed: ${processedCount} XML files. Skipped: ${skippedCount} files.`;
    }

    spinner.succeed(
      `Pipeline completed. Total processed: ${processedCount} XML files. Skipped: ${skippedCount} files.`,
    );
    db.close();
  });

  extractStream.on("error", (error) => {
    console.error("Error during extraction:", error);
  });

  try {
    const stream = fs
      .createReadStream(archivePath)
      .pipe(gunzip())
      .on("error", (err) => console.error("Gunzip error:", err))
      .pipe(extractStream)
      .on("error", (err) => console.error("Extract stream error:", err));

    // Handle SIGINT (CTRL+C) for cleanup
    process.on("SIGINT", () => {
      spinner.warn("SIGINT received. Cleaning up...");

      // Close the stream
      stream.destroy();

      // Wait for any remaining write operations to finish
      writeQueue.onIdle().then(() => {
        db.close();
        spinner.info("Database connection closed.");
        process.exit(0); // Exit the process
      });
    });
  } catch (error) {
    console.error("Error during pipeline execution:", error);
  }
}

// Function to insert data into the database in batches
function insertBatch(db, batch) {
  const insert = db.prepare(
    "INSERT OR IGNORE INTO data (sra_id, type, xml) VALUES (?, ?, ?)",
  );
  const transaction = db.transaction((batch) => {
    for (const record of batch) {
      insert.run(record.sra_id, record.type, record.xml);
    }
  });
  transaction(batch);
}

export default buildXmlDb;
