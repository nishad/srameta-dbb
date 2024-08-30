import { parentPort, workerData } from "worker_threads";
import { XMLParser } from "fast-xml-parser";

// Configure the XML parser
const alwaysArray = ["sample_set", "sample_set.sample.identifiers.external_id"];

const options = {
  ignoreAttributes: false,
  ignoreDeclaration: true,
  removeNSPrefix: false,
  attributeNamePrefix: "",
  textNodeName: "id",
  trimValues: true,
  transformTagName: (tagName) => tagName.toLowerCase(),
  isArray: (name, jpath, isLeafNode, isAttribute) => {
    return alwaysArray.includes(jpath);
  },
};

const parser = new XMLParser(options);

try {
  const parsedData = parser.parse(workerData.xml);
  const taxonIds = [];

  const extractTaxonIds = (obj) => {
    if (obj && typeof obj === "object") {
      for (const key in obj) {
        if (key.toLowerCase() === "taxon_id") {
          taxonIds.push(obj[key]);
        } else {
          extractTaxonIds(obj[key]);
        }
      }
    }
  };

  extractTaxonIds(parsedData);

  const jsonData = parsedData;

  parentPort.postMessage({ success: true, taxonIds, jsonData });
} catch (error) {
  parentPort.postMessage({ success: false, error: error.message });
}
