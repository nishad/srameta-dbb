import fs from "fs-extra";
import path from "path";
import axios from "axios";
import ora from "ora";
import EasyDl from "easydl";
import { fileURLToPath } from "url";

// Convert import.meta.url to a file path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_FOLDER = path.resolve(__dirname, "../../data");
const TMP_FOLDER = path.resolve(__dirname, "../../data/tmp");

export async function downloadLatestSRAFile() {
  try {
    // Ensure the data and temporary folders exist
    await fs.ensureDir(DATA_FOLDER);
    await fs.ensureDir(TMP_FOLDER);

    // Fetch the page content
    const response = await axios.get(
      "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/",
    );
    const pageContent = response.data;

    // Extract the latest file name using regex
    const regex = /NCBI_SRA_Metadata_Full_(\d{8})\.tar\.gz/g;
    let match;
    let latestFile = null;

    while ((match = regex.exec(pageContent)) !== null) {
      latestFile = match[0];
    }

    if (!latestFile) {
      console.error("No metadata files found on the page.");
      return;
    }

    const finalFilePath = path.join(DATA_FOLDER, latestFile);
    const tmpFilePath = path.join(TMP_FOLDER, latestFile);

    // Check if the file already exists in the final location
    if (await fs.pathExists(finalFilePath)) {
      console.log(
        `File already exists at ${finalFilePath}. Skipping download.`,
      );
      return;
    }

    // Start the download to the temporary location
    const downloadUrl = `${"https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/"}${latestFile}`;
    await startDownload(downloadUrl, tmpFilePath);

    // Move the file to the final location
    await fs.move(tmpFilePath, finalFilePath);
    console.log(`File moved to ${finalFilePath}`);
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

async function startDownload(downloadUrl, tmpFilePath) {
  console.log("Downloading ", downloadUrl);

  const spinner = ora(`Downloading ${path.basename(tmpFilePath)}`).start();

  const dl = new EasyDl(downloadUrl, tmpFilePath, {
    chunkSize: 200 * 1024 * 1024, // 200 MB chunk
  });

  dl.on("progress", (progress) => {
    spinner.text = `Progress: ${progress.total.percentage.toFixed(2)}%`;
  });

  const completed = await dl.wait();

  if (completed) {
    spinner.succeed(
      `Downloaded ${path.basename(tmpFilePath)} successfully to ${tmpFilePath}`,
    );
  } else {
    spinner.fail(
      `Download failed for ${path.basename(tmpFilePath)}. Please try again.`,
    );
  }
}

export default downloadLatestSRAFile;
