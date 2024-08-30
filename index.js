// index.js
import { main } from "./src/cli.js";
import figlet from "figlet";

// Wrap figlet in a Promise
function generateFiglet(text) {
  return new Promise((resolve, reject) => {
    figlet(text, function (err, data) {
      if (err) {
        return reject(err);
      }
      resolve(data);
    });
  });
}

async function start() {
  try {
    const figletText = await generateFiglet("SRAmetaDBB");
    console.log(figletText);

    // Now start the main prompts
    await main();
  } catch (err) {
    console.error("An error occurred:", err);
    process.exit(1);
  }
}

start();
