import got from "got";
import fs from "fs";
import path from "path";
import { exec } from "child_process";
import puppeteer from "puppeteer";
import dotenv from "dotenv";
import pLimit from "p-limit";

dotenv.config();

const DOWNLOAD_TIMEOUT = parseInt(process.env.DOWNLOAD_TIMEOUT || "120000", 10);
const MAX_CONCURRENCY = parseInt(process.env.MAX_CONCURRENCY || "5", 10);
const RETRY_LIMIT = parseInt(process.env.RETRY_LIMIT || "3", 10);
const USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
];

// Utility Functions
function validateInputFile(filePath) {
    if (!fs.existsSync(filePath) || !filePath.endsWith(".json")) {
        throw new Error(`Invalid file: ${filePath}`);
    }
}

// Utility Functions for State Management
function loadState(stateFile) {
    if (fs.existsSync(stateFile)) {
        return JSON.parse(fs.readFileSync(stateFile, "utf8"));
    }
    return { lastIndex: 0, tasks: [] };
}

function saveState(stateFile, state) {
    fs.writeFileSync(stateFile, JSON.stringify(state, null, 2), "utf8");
}

// Save state periodically
function autoSaveState(stateFile, state) {
    setInterval(() => {
        saveState(stateFile, state);
        console.log(`State saved to ${stateFile}`);
    }, 60000); // Every 1 minute
}

function appendToJsonFile(filePath, data) {
    let array = [];
    if (fs.existsSync(filePath)) {
        array = JSON.parse(fs.readFileSync(filePath, "utf8"));
    } else {
        const dir = path.dirname(filePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
    }

    array.push(data);
    fs.writeFileSync(filePath, JSON.stringify(array, null, 2), "utf8");
}

// Puppeteer for Cookie Retrieval
async function getCookies(url) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();

    try {
        console.log(`Fetching cookies for ${url}`);
        await page.goto(url, { waitUntil: "networkidle2" });
        const cookies = await page.cookies();
        return cookies.map(cookie => `${cookie.name}=${cookie.value}`).join("; ");
    } catch (error) {
        console.error(`Failed to fetch cookies for ${url}: ${error.message}`);
        throw error;
    } finally {
        await browser.close();
    }
}

// Got for HTTP Download with Retry
async function downloadWithGot(url, outputPath, userAgent, cookies = "") {
    for (let attempt = 1; attempt <= RETRY_LIMIT; attempt++) {
        try {
            const response = await got.stream(url, {
                timeout: { request: DOWNLOAD_TIMEOUT },
                headers: {
                    "User-Agent": userAgent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    Cookie: cookies,
                },
            });

            const writer = fs.createWriteStream(outputPath);

            // Handle errors in both the response stream and the writer stream
            return new Promise((resolve, reject) => {
                response.on("error", (error) => {
                    console.error(`Stream error for ${url}: ${error.message}`);
                    reject(error);
                });
    
                response.pipe(writer);

                writer.on("finish", () => {
                    console.log(`Successfully downloaded ${path.basename(outputPath)} using Got`);
                    resolve(true);
                });

                writer.on("error", (error) => {
                    console.error(`File write error for ${outputPath}: ${error.message}`);
                    reject(error);
                });
            });
        } catch (error) {
            if (error.response && error.response.statusCode === 503) {
                console.error(`503 Service Unavailable for ${url} (Attempt ${attempt})`);
                if (attempt < RETRY_LIMIT) {
                    const delay = Math.pow(2, attempt) * 1000; // Exponential backoff
                    console.log(`Retrying after ${delay}ms...`);
                    await new Promise((resolve) => setTimeout(resolve, delay));
                } else {
                    throw new Error("503 Service Unavailable (Max retries reached)");
                }
            } else {
                console.error(`Got attempt ${attempt} failed for ${url}: ${error.message}`);
                if (attempt === RETRY_LIMIT) throw error;
            }
        }
    }
}

// Puppeteer for Fallback Download
async function downloadWithPuppeteer(url, outputPath) {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();

    try {
        console.log(`Using Puppeteer to download ${url}`);
        await page.goto(url, { waitUntil: "networkidle2" });

        const pdfBuffer = await page.evaluate(async () => {
            const response = await fetch(document.location.href);
            if (response.ok) {
                return await response.arrayBuffer();
            }
            throw new Error(`Fetch failed with status ${response.status}`);
        });

        if (pdfBuffer) {
            fs.writeFileSync(outputPath, Buffer.from(pdfBuffer));
            console.log(`Successfully downloaded ${path.basename(outputPath)} using Puppeteer`);
        } else {
            throw new Error("Failed to fetch PDF from rendered page.");
        }
    } catch (error) {
        console.error(`Puppeteer failed for ${url}: ${error.message}`);
        throw error;
    } finally {
        await browser.close();
    }
}

// Main Download Process
async function downloadDatasheets(jsonData, outputFolder, stateFile) {
    let state = loadState(stateFile);
    const failedJsonPath = path.join(outputFolder, "failed.json");

    if (!fs.existsSync(outputFolder)) {
        fs.mkdirSync(outputFolder, { recursive: true });
    }

    const limit = pLimit(MAX_CONCURRENCY);

    const tasks = jsonData.slice(state.lastIndex).map((item, index) =>
        limit(async () => {
            const taskIndex = state.lastIndex + index; // Calculate task index
            const datasheetName = `${item.title.replace(/\//g, "-")}.pdf`;
            const outputPath = path.join(outputFolder, datasheetName);

            console.log(`Processing task ${taskIndex}: ${datasheetName} from ${item.url}`);

            let success = false;

            try {
                let cookies = "";
                try {
                    cookies = await getCookies(item.url);
                } catch (cookieError) {
                    console.error(`Failed to retrieve cookies: ${cookieError.message}`);
                }

                for (const userAgent of USER_AGENTS) {
                    try {
                        console.log(`Trying Got with User-Agent: ${userAgent}`);
                        await downloadWithGot(item.url, outputPath, userAgent, cookies);
                        success = true;
                        break; // Exit loop if Got succeeds
                    } catch (error) {
                        if (error.message === "404 Not Found") {
                            appendToJsonFile(failedJsonPath, {
                                id: item.id,
                                title: item.title,
                                url: item.url,
                                reason: "404 Not Found",
                            });
                            console.error(`Skipping ${item.url} due to 404 error.`);
                            break; // No retries for 404 errors
                        } else if (error.message === "503 Service Unavailable (Max retries reached)") {
                            appendToJsonFile(failedJsonPath, {
                                id: item.id,
                                title: item.title,
                                url: item.url,
                                reason: "503 Service Unavailable",
                            });
                            console.error(`Skipping ${item.url} due to 503 error.`);
                            break; // No retries for 503 errors
                        }
                        console.error(`Got failed for ${item.url} with User-Agent ${userAgent}: ${error.message}`);
                    }
                }

                if (!success) {
                    console.log("Fallback to Puppeteer");
                    await downloadWithPuppeteer(item.url, outputPath);
                    success = true;
                }

                state.tasks.push({ id: item.id, url: item.url, status: "completed" });

            } catch (error) {
                console.error(`All methods failed for ${item.url}: ${error.message}`);
                state.tasks.push({ id: item.id, url: item.url, status: "failed", reason: error.message });
                appendToJsonFile(failedJsonPath, {
                    id: item.id,
                    title: item.title,
                    url: item.url,
                    reason: error.message,
                });
            } 

            if (!success) console.log(`Skipping failed instance: ${item.url}`);
            state.lastIndex = taskIndex + 1;

            // Save state after each task
            saveState(stateFile, state);
            
        })
    );

    await Promise.all(tasks);
    console.log("All datasheets processed.");
}

// Main Script
(async () => {
    const inputFolder = "./input";
    const outputFolder = "./output";
    const finishedFolder = "./finished";

    if (!fs.existsSync(finishedFolder)) {
        fs.mkdirSync(finishedFolder, { recursive: true });
    }

    const inputFiles = fs.readdirSync(inputFolder).filter(file => file.startsWith("invalid_datasheet_urls_"));

    for (const inputFile of inputFiles) {
        const categorySlug = inputFile.match(/invalid_datasheet_urls_(.+)\.json/)[1];
        const fullPath = path.join(inputFolder, inputFile);
        const finishedPath = path.join(finishedFolder, inputFile);
        const stateFile = path.join(outputFolder, `state_${categorySlug}.json`);
        const categoryOutputFolder = path.join(outputFolder, `datasheet_${categorySlug}`);

        if (!fs.existsSync(categoryOutputFolder)) fs.mkdirSync(categoryOutputFolder, { recursive: true });

        try {
            validateInputFile(fullPath);
            console.log("Loading file:", fullPath);

            const jsonData = JSON.parse(fs.readFileSync(fullPath, "utf8"));
            const state = loadState(stateFile); // Properly load state here

            //autoSaveState(stateFile, loadState(stateFile));
            autoSaveState(stateFile, state); 

            await downloadDatasheets(jsonData, categoryOutputFolder, stateFile);
            fs.renameSync(fullPath, finishedPath);
            console.log(`File moved to finished folder: ${finishedPath}`);
        } catch (error) {
            console.error(`Failed to process file: ${fullPath}`, error);
        }
    }
})();
