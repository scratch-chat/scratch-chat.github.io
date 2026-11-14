const { spawn } = require("node:child_process");
const { once } = require("node:events");
const path = require("node:path");
const { afterEach, beforeEach, test } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const WebSocket = require("../../vendor/ws.cjs");

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
  const ownerToken = randomUUID();
  return opened.then(() => {
    socket.send(JSON.stringify({ type: "identify", token: ownerToken }));
    return { socket, firstPayload, ownerToken };
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
  assert.match(pageHtml, /id="media-input"/);
  assert.match(pageHtml, /15 \* 60/);

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
  const firstLockState = await firstLock;
  const secondLockState = await secondLock;
  assert.equal(firstLockState.type, "lock_state");
  assert.equal(firstLockState.locked, true);
  assert.equal(secondLockState.type, "lock_state");
  assert.equal(secondLockState.locked, false);
  assert.equal(secondLockState.isLockOwner, true);

  const firstClear = nextPayloadOfType(first, "messages_cleared");
  const secondClear = nextPayloadOfType(second, "messages_cleared");
  first.send(JSON.stringify({ type: "delete_all_messages" }));
  assert.deepEqual(await firstClear, { type: "messages_cleared" });
  assert.deepEqual(await secondClear, { type: "messages_cleared" });
});

test("uploads an image, shares it in chat, and serves byte ranges", async (t) => {
  const { socket, firstPayload, ownerToken } = await connectWebSocket();
  t.after(() => socket.close());
  await firstPayload;

  const imageBytes = Buffer.from("sample-image-content");
  const uploadResponse = await fetch(`${baseUrl}/api/uploads?name=sample.png`, {
    method: "POST",
    headers: {
      "Content-Type": "image/png",
      "X-Owner-Token": ownerToken,
    },
    body: imageBytes,
  });
  assert.equal(uploadResponse.status, 201);
  const media = await uploadResponse.json();
  assert.equal(media.type, "image");
  assert.equal(media.name, "sample.png");

  const partialMediaResponse = await fetch(`${baseUrl}${media.url}`, {
    headers: { Range: "bytes=0-5" },
  });
  assert.equal(partialMediaResponse.status, 206);
  assert.equal(partialMediaResponse.headers.get("content-range"), `bytes 0-5/${imageBytes.length}`);
  assert.deepEqual(Buffer.from(await partialMediaResponse.arrayBuffer()), imageBytes.subarray(0, 6));

  const incomingMessage = nextPayloadOfType(socket, "message");
  socket.send(JSON.stringify({
    type: "message",
    name: "画像投稿者",
    text: "",
    mediaId: media.id,
  }));
  const message = await incomingMessage;
  assert.equal(message.text, "");
  assert.equal(message.media.url, media.url);
  assert.equal(message.media.mimeType, "image/png");
});

test("rejects videos longer than 15 minutes", async () => {
  const response = await fetch(`${baseUrl}/api/uploads?name=long.mp4`, {
    method: "POST",
    headers: {
      "Content-Type": "video/mp4",
      "X-Media-Duration": "901",
      "X-Owner-Token": randomUUID(),
    },
  });
  assert.equal(response.status, 400);
  assert.match((await response.json()).error, /15分以内/);
});

test("admin lock does not lock the owner but shows a lock status badge", async (t) => {
  const { socket: first, firstPayload: firstInitPromise } = await connectWebSocket("203.0.113.42");
  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket("198.51.100.15");
  t.after(() => { first.close(); second.close(); });
  await Promise.all([firstInitPromise, secondInitPromise]);

  const firstLock = nextPayloadOfType(first, "lock_state");
  const secondLock = nextPayloadOfType(second, "lock_state");
  second.send(JSON.stringify({ type: "admin_lock" }));

  const firstLockState = await firstLock;
  const secondLockState = await secondLock;
  assert.equal(firstLockState.locked, true);
  assert.equal(firstLockState.isLockOwner, false);
  assert.equal(secondLockState.locked, false);
  assert.equal(secondLockState.isLockOwner, true);
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
  const firstLockState = await firstLock;
  const secondLockState = await secondLock;
  assert.equal(firstLockState.type, "lock_state");
  assert.equal(firstLockState.locked, false);
  assert.equal(firstLockState.isLockOwner, true);
  assert.equal(secondLockState.type, "lock_state");
  assert.equal(secondLockState.locked, true);
  assert.equal(secondLockState.isLockOwner, false);

  const firstReset = nextPayloadOfType(first, "messages_cleared");
  const secondReset = nextPayloadOfType(second, "messages_cleared");
  const firstResetLock = nextPayloadOfType(first, "lock_state");
  const secondResetLock = nextPayloadOfType(second, "lock_state");
  second.send(JSON.stringify({ type: "reset_chat" }));
  assert.deepEqual(await firstReset, { type: "messages_cleared" });
  assert.deepEqual(await secondReset, { type: "messages_cleared" });
  const firstResetLockState = await firstResetLock;
  const secondResetLockState = await secondResetLock;
  assert.equal(firstResetLockState.type, "lock_state");
  assert.equal(firstResetLockState.locked, false);
  assert.equal(secondResetLockState.type, "lock_state");
  assert.equal(secondResetLockState.locked, false);
});
