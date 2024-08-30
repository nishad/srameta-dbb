// cli.js
import {
  intro,
  outro,
  select,
  text,
  confirm,
  isCancel,
  spinner,
  note,
} from "@clack/prompts";

import { downloadLatestSRAFile } from "./download/sra-archive.js";
import { downloadSRAAccessionsFile } from "./download/sra-accessions.js";
import { buildXmlDb } from "./xml-db/xml-db-builder.js";
import { buildAccessionsDb } from "./accessions-db/accessions-db-builder.js";
import { buildSamplesDb } from "./samples-db/samples-db-builder.js";

export async function main() {
  intro("A Database Builder for SRA Metadata.");

  const action = await select({
    message: "What would you like to do?",
    options: [
      {
        value: "download_archive",
        label: "Download latest SRA metadata archive",
      },
      {
        value: "generate_xml_db",
        label: "Create or update the XML SQLite database",
      },
      {
        value: "download_accessions",
        label: "Download latest SRA Accessions TAB file",
      },
      {
        value: "genrate_accessions_db",
        label: "Create or update the SRA Accessions SQLite database",
      },
      {
        value: "genrate_sample_db",
        label: "Create or update the SRA Samples SQLite database",
      },
    ],
  });

  if (isCancel(action)) {
    outro("Operation cancelled.");
    process.exit(0);
  }

  const confirmed = await confirm({
    message: "Do you want to proceed with the selected action?",
  });

  if (isCancel(confirmed)) {
    outro("Operation cancelled.");
    process.exit(0);
  }

  if (confirmed) {
    performAction(action);
  }

  // outro("Operation completed successfully.");
}

function performAction(action, input) {
  // console.log(`Action: ${action}`);
  // console.log(`Input: ${input}`);
  // console.log("This is a dummy function. Implement your logic here.");
  //
  // create a switch statement to call the appropriate function based on the action
  switch (action) {
    case "download_archive":
      downloadLatestSRAFile();
      break;

    case "generate_xml_db":
      buildXmlDb();
      break;

    case "download_accessions":
      downloadSRAAccessionsFile();
      break;

    case "genrate_accessions_db":
      buildAccessionsDb();
      break;

    case "genrate_sample_db":
      buildSamplesDb();
      break;

    default:
      console.log("This action is not yet implemented.");
  }
}

// Check if the script is run directly:
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
