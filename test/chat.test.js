const { spawn } = require("node:child_process");
const { once } = require("node:events");
const path = require("node:path");
const { after, before, test } = require("node:test");
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const WebSocket = require("../vendor/ws.cjs");

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
  return opened.then(() => {
    socket.send(JSON.stringify({ type: "identify", token: randomUUID() }));
    return { socket, firstPayload };
  });
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
  assert.match(pageHtml, /id="clear-chat-button"/);
  assert.match(pageHtml, /id="reply-preview"/);
  assert.match(pageHtml, /reaction_update/);
  assert.match(pageHtml, /"reset_chat"/);
  assert.equal((pageHtml.match(/data:image\/svg\+xml;base64,/g) || []).length, 2);

  const { socket: first, firstPayload: firstInitPromise } = await connectWebSocket();
  t.after(() => first.close());
  const firstInit = await firstInitPromise;
  assert.equal(firstInit.type, "init");
  assert.equal(firstInit.participants, 1);
  assert.equal(firstInit.canDeleteAll, true);
  assert.equal((await nextPayload(first)).count, 1);

  const participantUpdate = nextPayload(first);
  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket(otherIp);
  t.after(() => second.close());
  const secondInit = await secondInitPromise;
  assert.equal(secondInit.participants, 2);
  assert.equal(secondInit.canDeleteAll, false);
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
  assert.equal(toFirst.canDelete, false);
  assert.equal(toSecond.canDelete, true);
  assert.deepEqual(toFirst.reactions, { heart: 0, star: 0 });
  assert.deepEqual(toSecond.myReactions, { heart: false, star: false });

  second.send(JSON.stringify({ type: "admin_lock" }));
  second.send(JSON.stringify({ type: "delete_all_messages" }));
  second.send(JSON.stringify({ type: "reset_chat" }));
  first.send(JSON.stringify({ type: "delete_message", id: toFirst.id }));

  const { socket: third, firstPayload: thirdInitPromise } = await connectWebSocket(otherIp);
  t.after(() => third.close());
  const thirdInit = await thirdInitPromise;
  assert.equal(thirdInit.locked, false);
  assert.equal(thirdInit.messages.length, 1);
  assert.equal(thirdInit.messages[0].id, toFirst.id);
  assert.equal(thirdInit.messages[0].canDelete, false);
  assert.equal(thirdInit.canDeleteAll, false);
  assert.equal((await nextPayload(third)).count, 3);
  assert.equal((await nextPayload(first)).count, 3);
  assert.equal((await nextPayload(second)).count, 3);

  const firstHeartUpdate = nextPayload(first);
  const secondHeartUpdate = nextPayload(second);
  const thirdHeartUpdate = nextPayload(third);
  second.send(JSON.stringify({ type: "reaction", id: toFirst.id, reaction: "heart" }));
  const [heartForFirst, heartForSecond, heartForThird] = await Promise.all([
    firstHeartUpdate,
    secondHeartUpdate,
    thirdHeartUpdate,
  ]);
  assert.deepEqual(heartForFirst.reactions, { heart: 1, star: 0 });
  assert.deepEqual(heartForFirst.myReactions, { heart: false, star: false });
  assert.deepEqual(heartForSecond.myReactions, { heart: true, star: false });
  assert.deepEqual(heartForThird.myReactions, { heart: false, star: false });

  const firstStarUpdate = nextPayload(first);
  const secondStarUpdate = nextPayload(second);
  const thirdStarUpdate = nextPayload(third);
  first.send(JSON.stringify({ type: "reaction", id: toFirst.id, reaction: "star" }));
  const [starForFirst, starForSecond, starForThird] = await Promise.all([
    firstStarUpdate,
    secondStarUpdate,
    thirdStarUpdate,
  ]);
  assert.deepEqual(starForFirst.reactions, { heart: 1, star: 1 });
  assert.deepEqual(starForFirst.myReactions, { heart: false, star: true });
  assert.deepEqual(starForSecond.myReactions, { heart: true, star: false });
  assert.deepEqual(starForThird.myReactions, { heart: false, star: false });

  const firstHeartToggle = nextPayload(first);
  const secondHeartToggle = nextPayload(second);
  const thirdHeartToggle = nextPayload(third);
  second.send(JSON.stringify({ type: "reaction", id: toFirst.id, reaction: "heart" }));
  const [heartToggleForFirst, heartToggleForSecond, heartToggleForThird] = await Promise.all([
    firstHeartToggle,
    secondHeartToggle,
    thirdHeartToggle,
  ]);
  assert.deepEqual(heartToggleForFirst.reactions, { heart: 0, star: 1 });
  assert.deepEqual(heartToggleForSecond.myReactions, { heart: false, star: false });
  assert.deepEqual(heartToggleForThird.reactions, { heart: 0, star: 1 });

  const firstReply = nextPayload(first);
  const secondReply = nextPayload(second);
  const thirdReply = nextPayload(third);
  first.send(JSON.stringify({
    type: "message",
    name: "管理者",
    text: "返信テスト",
    color: "#f7faff",
    replyToId: toFirst.id,
  }));
  const [replyForFirst, replyForSecond, replyForThird] = await Promise.all([
    firstReply,
    secondReply,
    thirdReply,
  ]);
  assert.deepEqual(replyForFirst.reply, {
    id: toFirst.id,
    name: "テスト参加者",
    text: "こんにちは",
  });
  assert.deepEqual(replyForSecond.reply, replyForFirst.reply);
  assert.deepEqual(replyForThird.reply, replyForFirst.reply);

  const firstDelete = nextPayload(first);
  const secondDelete = nextPayload(second);
  const thirdDelete = nextPayload(third);
  second.send(JSON.stringify({ type: "delete_message", id: toFirst.id }));
  const [deletedForFirst, deletedForSecond, deletedForThird] = await Promise.all([
    firstDelete,
    secondDelete,
    thirdDelete,
  ]);
  assert.deepEqual(deletedForFirst, { type: "message_deleted", id: toFirst.id });
  assert.deepEqual(deletedForSecond, deletedForFirst);
  assert.deepEqual(deletedForThird, deletedForFirst);

  const newMessageForFirst = nextPayload(first);
  const newMessageForSecond = nextPayload(second);
  const newMessageForThird = nextPayload(third);
  second.send(JSON.stringify({
    type: "message",
    name: "テスト参加者",
    text: "全員分の削除テスト",
    color: "#fffafa",
  }));
  const [messageForFirst, messageForSecond, messageForThird] = await Promise.all([
    newMessageForFirst,
    newMessageForSecond,
    newMessageForThird,
  ]);
  assert.equal(messageForFirst.text, "全員分の削除テスト");
  assert.equal(messageForSecond.canDelete, true);
  assert.equal(messageForThird.canDelete, false);

  const firstCleared = nextPayload(first);
  const secondCleared = nextPayload(second);
  const thirdCleared = nextPayload(third);
  first.send(JSON.stringify({ type: "delete_all_messages" }));
  const [clearedForFirst, clearedForSecond, clearedForThird] = await Promise.all([
    firstCleared,
    secondCleared,
    thirdCleared,
  ]);
  assert.deepEqual(clearedForFirst, { type: "messages_cleared" });
  assert.deepEqual(clearedForSecond, clearedForFirst);
  assert.deepEqual(clearedForThird, clearedForFirst);

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

  const resetMessageForFirst = nextPayload(first);
  const resetMessageForSecond = nextPayload(second);
  const resetMessageForThird = nextPayload(third);
  second.send(JSON.stringify({
    type: "message",
    name: "テスト参加者",
    text: "初期化テスト",
    color: "#fffafa",
  }));
  const resetTestMessages = await Promise.all([
    resetMessageForFirst,
    resetMessageForSecond,
    resetMessageForThird,
  ]);
  assert.equal(resetTestMessages[0].text, "初期化テスト");

  const firstRelock = nextPayload(first);
  const secondRelock = nextPayload(second);
  const thirdRelock = nextPayload(third);
  first.send(JSON.stringify({ type: "admin_lock" }));
  assert.deepEqual(await firstRelock, { type: "lock_state", locked: true });
  assert.deepEqual(await secondRelock, { type: "lock_state", locked: true });
  assert.deepEqual(await thirdRelock, { type: "lock_state", locked: true });

  const firstResetMessages = nextPayload(first);
  const firstResetLock = nextPayload(first);
  const secondResetMessages = nextPayload(second);
  const secondResetLock = nextPayload(second);
  const thirdResetMessages = nextPayload(third);
  const thirdResetLock = nextPayload(third);
  first.send(JSON.stringify({ type: "reset_chat" }));
  assert.deepEqual(await firstResetMessages, { type: "messages_cleared" });
  assert.deepEqual(await firstResetLock, { type: "lock_state", locked: false });
  assert.deepEqual(await secondResetMessages, { type: "messages_cleared" });
  assert.deepEqual(await secondResetLock, { type: "lock_state", locked: false });
  assert.deepEqual(await thirdResetMessages, { type: "messages_cleared" });
  assert.deepEqual(await thirdResetLock, { type: "lock_state", locked: false });
});
