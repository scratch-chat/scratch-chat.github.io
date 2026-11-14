const { spawn } = require("node:child_process");
const { once } = require("node:events");
const path = require("node:path");
const { after, before, test } = require("node:test");
const assert = require("node:assert/strict");
const WebSocket = require("ws");

const root = path.join(__dirname, "..");
const allowedIp = "203.0.113.42";
const otherIp = "198.51.100.15";
let serverProcess;
let baseUrl;
const payloadReaders = new WeakMap();

before(async () => {
  serverProcess = spawn(process.execPath, ["server.js"], {
    cwd: root,
    env: { ...process.env, PORT: "0", ADMIN_ALLOWED_IP: allowedIp },
    stdio: ["ignore", "pipe", "inherit"],
  });

  const address = await new Promise((resolve, reject) => {
    let output = "";
    serverProcess.stdout.setEncoding("utf8");
    serverProcess.stdout.on("data", (chunk) => {
      output += chunk;
      const match = output.match(/http:\/\/localhost:(\d+)/);
      if (match) {
        resolve(match[1]);
      }
    });
    serverProcess.once("error", reject);
    serverProcess.once("exit", (code) => {
      reject(new Error(`Chat server exited before starting (code ${code})`));
    });
  });

  baseUrl = `http://localhost:${address}`;
});

after(async () => {
  if (serverProcess && serverProcess.exitCode === null) {
    serverProcess.kill("SIGTERM");
    await once(serverProcess, "exit");
  }
});

function nextPayload(socket) {
  let reader = payloadReaders.get(socket);
  if (!reader) {
    const queue = [];
    const waiters = [];
    socket.on("message", (data) => {
      const payload = JSON.parse(data.toString());
      const resolve = waiters.shift();
      if (resolve) {
        resolve(payload);
      } else {
        queue.push(payload);
      }
    });
    reader = () => queue.length
      ? Promise.resolve(queue.shift())
      : new Promise((resolve) => waiters.push(resolve));
    payloadReaders.set(socket, reader);
  }
  return reader();
}

function connectWebSocket(clientIp = allowedIp) {
  const socket = new WebSocket(baseUrl.replace("http:", "ws:") + "/ws", {
    headers: { "x-forwarded-for": clientIp },
  });
  const opened = once(socket, "open");
  const firstPayload = nextPayload(socket);
  return opened.then(() => ({ socket, firstPayload }));
}

test("serves the chat and broadcasts colored messages to all participants", async (t) => {
  const page = await fetch(baseUrl);
  assert.equal(page.status, 200);
  const pageHtml = await page.text();
  assert.match(pageHtml, /Scratch チャット/);
  assert.match(pageHtml, /id="lock-screen"/);
  assert.match(pageHtml, /<style>/);
  assert.match(pageHtml, /function connect\(\)/);
  assert.match(pageHtml, /window\.CHAT_WEBSOCKET_URL/);
  assert.equal((pageHtml.match(/data:image\/svg\+xml;base64,/g) || []).length, 2);

  const { socket: first, firstPayload: firstInitPromise } = await connectWebSocket();
  t.after(() => first.close());
  const firstInit = await firstInitPromise;
  assert.equal(firstInit.type, "init");
  assert.equal(firstInit.participants, 1);
  assert.equal((await nextPayload(first)).count, 1);

  const participantUpdate = nextPayload(first);
  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket(otherIp);
  t.after(() => second.close());
  const secondInit = await secondInitPromise;
  assert.equal(secondInit.participants, 2);
  assert.equal((await participantUpdate).count, 2);
  assert.equal((await nextPayload(second)).type, "participants");

  const firstMessage = nextPayload(first);
  const secondMessage = nextPayload(second);
  second.send(JSON.stringify({
    type: "message",
    name: "テスト参加者",
    text: "こんにちは",
    color: "#fffafa",
  }));

  const [toFirst, toSecond] = await Promise.all([firstMessage, secondMessage]);
  assert.equal(toFirst.text, "こんにちは");
  assert.equal(toFirst.name, "テスト参加者");
  assert.equal(toFirst.color, "#fffafa");
  assert.equal(toSecond.id, toFirst.id);

  second.send(JSON.stringify({ type: "admin_lock" }));

  const { socket: third, firstPayload: thirdInitPromise } = await connectWebSocket(otherIp);
  t.after(() => third.close());
  const thirdInit = await thirdInitPromise;
  assert.equal(thirdInit.locked, false);
  assert.equal((await nextPayload(third)).count, 3);
  assert.equal((await nextPayload(first)).count, 3);
  assert.equal((await nextPayload(second)).count, 3);

  const firstLockUpdate = nextPayload(first);
  const secondLockUpdate = nextPayload(second);
  const thirdLockUpdate = nextPayload(third);
  first.send(JSON.stringify({ type: "admin_lock" }));
  const [firstLock, secondLock, thirdLock] = await Promise.all([
    firstLockUpdate,
    secondLockUpdate,
    thirdLockUpdate,
  ]);
  assert.deepEqual(firstLock, { type: "lock_state", locked: true });
  assert.deepEqual(secondLock, firstLock);
  assert.deepEqual(thirdLock, firstLock);

  const firstUnlock = nextPayload(first);
  const secondUnlock = nextPayload(second);
  const thirdUnlock = nextPayload(third);
  first.send(JSON.stringify({ type: "admin_lock" }));
  assert.deepEqual(await firstUnlock, { type: "lock_state", locked: false });
  assert.deepEqual(await secondUnlock, { type: "lock_state", locked: false });
  assert.deepEqual(await thirdUnlock, { type: "lock_state", locked: false });
});
