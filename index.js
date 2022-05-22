const fs = require("fs");

const cardsData = fs.readFileSync("./cards.json");
const cards = JSON.parse(cardsData);
const allCards = cards.map((c) => {
  return Buffer.from(JSON.stringify(c));
});
const allCardsLength = allCards.length;
const userSawAllCards = Buffer.from(JSON.stringify({ id: "ALL CARDS" }));

const port = +process.argv[2] || 3000;
const lockFile = "./master.lock";
let isMaster = true;
try {
  fs.writeFileSync(lockFile, `${port}`, { flag: "wx" });
} catch (err) {
  if (err.message.startsWith("EEXIST: file already exists")) {
    isMaster = false;
  } else {
    console.log("Master Lock Error", err);
    throw err;
  }
}

let masterPort;
if (!isMaster) {
  masterPortStr = fs.readFileSync(lockFile, "utf8");
  masterPort = parseInt(masterPortStr, 10);
  fs.unlinkSync(lockFile);
}

const shutdownHandler = (signal) => {
  console.log("starting shutdown, got signal " + signal);
  if (isMaster) {
    try {
      fs.unlinkSync(lockFile);
    } catch (err) {
      console.log(
        "failed to delete lockfile probably because it's already been deleted",
        err
      );
    }
  }
  process.exit(0);
};

process.on("SIGINT", shutdownHandler);
process.on("SIGTERM", shutdownHandler);

const userIndexes = {};

const router = async (req, res) => {
  res.statusCode = 200;

  if (!userIndexes[req.url]) {
    userIndexes[req.url] = 0;
  }
  const idx = ++userIndexes[req.url];

  if (idx <= allCardsLength) {
    res.end(allCards[idx - 1]);
    return;
  }
  res.end(userSawAllCards);
  return;
};

/* Define the servers and start listening to requests */

const net = require("net");
const forwarder = net.createServer((from) => {
  const to = net.createConnection({
    host: "0.0.0.0",
    port: masterPort,
  });
  from.pipe(to);
  to.pipe(from);
});

const http = require("turbo-http");
let server = http.createServer();

if (!isMaster) {
  server = forwarder;
}

server.on("request", router);
server.listen(port, "0.0.0.0", () => {
  console.log(`Server listening at http://0.0.0.0:${port}`);
});
