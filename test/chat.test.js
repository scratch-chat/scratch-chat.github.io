const { spawn } = require("node:child_process");
const { once } = require("node:events");
const path = require("node:path");
const { afterEach, beforeEach, test } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const WebSocket = require("../vendor/ws.cjs");

const root = path.join(__dirname, "..");
let serverProcess;
let baseUrl;
const payloadReaders = new WeakMap();

beforeEach(async () => {
  serverProcess = spawn(process.execPath, ["server.js"], {
    cwd: root,
    env: { ...process.env, PORT: "0" },
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

afterEach(async () => {
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

async function nextPayloadOfType(socket, type) {
  for (;;) {
    const payload = await nextPayload(socket);
    if (payload.type === type) {
      return payload;
    }
  }
}

function connectWebSocket(clientIp = "203.0.113.42") {
  const socket = new WebSocket(baseUrl.replace("http:", "ws:") + "/ws", {
    headers: { "x-forwarded-for": clientIp },
  });
  const opened = once(socket, "open");
  const firstPayload = nextPayload(socket);
  return opened.then(() => {
    socket.send(JSON.stringify({ type: "identify", token: randomUUID() }));
    return { socket, firstPayload };
  });
}

test("serves the chat page and exposes admin controls to every client", async (t) => {
  const page = await fetch(baseUrl);
  assert.equal(page.status, 200);
  const pageHtml = await page.text();
  assert.match(pageHtml, /Scratch チャット/);
  assert.match(pageHtml, /id="lock-screen"/);
  assert.match(pageHtml, /window\.CHAT_WEBSOCKET_URL/);
  assert.match(pageHtml, /"reset_chat"/);

  const { socket: first, firstPayload: firstInitPromise } = await connectWebSocket("203.0.113.42");
  t.after(() => first.close());
  const firstInit = await firstInitPromise;
  assert.equal(firstInit.type, "init");
  assert.equal(firstInit.participants, 1);
  assert.equal(firstInit.canDeleteAll, true);

  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket("198.51.100.15");
  t.after(() => second.close());
  const secondInit = await secondInitPromise;
  assert.equal(secondInit.participants, 2);
  assert.equal(secondInit.canDeleteAll, true);

  const firstMessage = nextPayloadOfType(first, "message");
  const secondMessage = nextPayloadOfType(second, "message");
  second.send(JSON.stringify({
    type: "message",
    name: "参加者2",
    text: "hello",
    color: "#fffafa",
  }));

  const [toFirst, toSecond] = await Promise.all([firstMessage, secondMessage]);
  assert.equal(toFirst.text, "hello");
  assert.equal(toFirst.canDelete, false);
  assert.equal(toSecond.canDelete, true);

  const firstDelete = nextPayloadOfType(first, "message_deleted");
  const secondDelete = nextPayloadOfType(second, "message_deleted");
  second.send(JSON.stringify({ type: "delete_message", id: toFirst.id }));
  const deleted = await Promise.all([firstDelete, secondDelete]);
  assert.deepEqual(deleted[0], { type: "message_deleted", id: toFirst.id });
  assert.deepEqual(deleted[1], { type: "message_deleted", id: toFirst.id });

  const firstLock = nextPayloadOfType(first, "lock_state");
  const secondLock = nextPayloadOfType(second, "lock_state");
  second.send(JSON.stringify({ type: "admin_lock" }));
  assert.deepEqual(await firstLock, { type: "lock_state", locked: true });
  assert.deepEqual(await secondLock, { type: "lock_state", locked: true });

  const firstClear = nextPayloadOfType(first, "messages_cleared");
  const secondClear = nextPayloadOfType(second, "messages_cleared");
  first.send(JSON.stringify({ type: "delete_all_messages" }));
  assert.deepEqual(await firstClear, { type: "messages_cleared" });
  assert.deepEqual(await secondClear, { type: "messages_cleared" });
});

test("reset_chat clears history and unlocks the room without any auth gate", async (t) => {
  const { socket: first, firstPayload: firstInitPromise } = await connectWebSocket("203.0.113.42");
  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket("198.51.100.15");
  t.after(() => { first.close(); second.close(); });
  await Promise.all([firstInitPromise, secondInitPromise]);

  const firstMessage = nextPayloadOfType(first, "message");
  const secondMessage = nextPayloadOfType(second, "message");
  second.send(JSON.stringify({
    type: "message",
    name: "参加者2",
    text: "reset test",
    color: "#fffafa",
  }));
  const [toFirst, toSecond] = await Promise.all([firstMessage, secondMessage]);
  assert.equal(toFirst.text, "reset test");
  assert.equal(toSecond.text, "reset test");

  const firstLock = nextPayloadOfType(first, "lock_state");
  const secondLock = nextPayloadOfType(second, "lock_state");
  first.send(JSON.stringify({ type: "admin_lock" }));
  assert.deepEqual(await firstLock, { type: "lock_state", locked: true });
  assert.deepEqual(await secondLock, { type: "lock_state", locked: true });

  const firstReset = nextPayloadOfType(first, "messages_cleared");
  const secondReset = nextPayloadOfType(second, "messages_cleared");
  const firstResetLock = nextPayloadOfType(first, "lock_state");
  const secondResetLock = nextPayloadOfType(second, "lock_state");
  second.send(JSON.stringify({ type: "reset_chat" }));
  assert.deepEqual(await firstReset, { type: "messages_cleared" });
  assert.deepEqual(await secondReset, { type: "messages_cleared" });
  assert.deepEqual(await firstResetLock, { type: "lock_state", locked: false });
  assert.deepEqual(await secondResetLock, { type: "lock_state", locked: false });
});
