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

async function createRoom(ownerToken = randomUUID()) {
  const response = await fetch(`${baseUrl}/api/rooms`, {
    method: "POST",
    headers: { "X-Owner-Token": ownerToken },
  });
  assert.equal(response.status, 201);
  return response.json();
}

async function connectWebSocket(clientIp = "203.0.113.42", roomCode) {
  const ownerToken = randomUUID();
  let createdRoom;
  if (!roomCode) {
    createdRoom = await createRoom(ownerToken);
    roomCode = createdRoom.code;
  }

  const socketUrl = new URL("/ws", baseUrl.replace("http:", "ws:"));
  socketUrl.searchParams.set("room", roomCode);
  const socket = new WebSocket(socketUrl.toString(), {
    headers: { "x-forwarded-for": clientIp },
  });
  const opened = once(socket, "open");
  const firstPayload = nextPayload(socket);
  await opened;
  socket.send(JSON.stringify({ type: "identify", token: ownerToken }));
  return {
    socket,
    firstPayload,
    ownerToken,
    roomCode,
    commandLetters: createdRoom?.commandLetters || "",
  };
}

test("serves the chat page and keeps admin controls with the room creator", async (t) => {
  const page = await fetch(baseUrl);
  assert.equal(page.status, 200);
  const pageHtml = await page.text();
  assert.match(pageHtml, /Scratch チャット/);
  assert.match(pageHtml, /id="lock-screen"/);
  assert.match(pageHtml, /id="join-form"/);
  assert.match(pageHtml, /id="room-code-input"/);
  assert.match(pageHtml, /id="create-room-button"/);
  assert.match(pageHtml, /<hr class="join-divider">/);
  assert.match(pageHtml, /inputmode="numeric"/);
  assert.match(pageHtml, /id="active-room-code"/);
  assert.match(pageHtml, /id="creator-notice"/);
  assert.match(pageHtml, /id="disband-room-button"[^>]*>解散<\/button>/);
  assert.match(pageHtml, /\.creator-command-bar\s*\{[^}]*justify-content:\s*space-between;/s);
  assert.match(pageHtml, /\.disband-room-button\s*\{[^}]*color:\s*#c62828;/s);
  assert.match(pageHtml, /id="chat" hidden/);
  assert.match(pageHtml, /\.join-screen\[hidden\]\s*\{\s*display:\s*none;/);
  assert.match(pageHtml, /window\.CHAT_WEBSOCKET_URL/);
  assert.match(pageHtml, /"reset_chat"/);
  assert.match(pageHtml, /id="media-input"/);
  assert.match(pageHtml, /15 \* 60/);

  const {
    socket: first,
    firstPayload: firstInitPromise,
    roomCode: firstRoomCode,
    commandLetters,
  } = await connectWebSocket("203.0.113.42");
  t.after(() => first.close());
  const firstInit = await firstInitPromise;
  assert.equal(firstInit.type, "init");
  assert.equal(firstInit.participants, 1);
  assert.equal(firstInit.canDeleteAll, true);

  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket("198.51.100.15", firstRoomCode);
  t.after(() => second.close());
  const secondInit = await secondInitPromise;
  assert.equal(secondInit.participants, 2);
  assert.equal(secondInit.canDeleteAll, false);

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
  first.send(JSON.stringify({ type: "admin_lock", commandLetters }));
  const firstLockState = await firstLock;
  const secondLockState = await secondLock;
  assert.equal(firstLockState.type, "lock_state");
  assert.equal(firstLockState.locked, false);
  assert.equal(firstLockState.isLockOwner, true);
  assert.equal(secondLockState.type, "lock_state");
  assert.equal(secondLockState.locked, true);
  assert.equal(secondLockState.isLockOwner, false);

  const firstClear = nextPayloadOfType(first, "messages_cleared");
  const secondClear = nextPayloadOfType(second, "messages_cleared");
  first.send(JSON.stringify({ type: "delete_all_messages", commandLetters }));
  assert.deepEqual(await firstClear, { type: "messages_cleared" });
  assert.deepEqual(await secondClear, { type: "messages_cleared" });
});

test("only the creator can disband a room and immediately release its code and history", async (t) => {
  const {
    socket: creator,
    firstPayload: creatorInitPromise,
    roomCode,
    commandLetters,
  } = await connectWebSocket();
  const {
    socket: participant,
    firstPayload: participantInitPromise,
  } = await connectWebSocket("198.51.100.15", roomCode);
  t.after(() => { creator.close(); participant.close(); });
  await Promise.all([creatorInitPromise, participantInitPromise]);

  participant.send(JSON.stringify({ type: "disband_room", commandLetters }));
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(creator.readyState, WebSocket.OPEN);
  assert.equal(participant.readyState, WebSocket.OPEN);

  const creatorMessage = nextPayloadOfType(creator, "message");
  const participantMessage = nextPayloadOfType(participant, "message");
  participant.send(JSON.stringify({
    type: "message",
    name: "参加者",
    text: "消える履歴",
    color: "#fffafa",
  }));
  await Promise.all([creatorMessage, participantMessage]);

  const creatorDisbanded = nextPayloadOfType(creator, "room_disbanded");
  const participantDisbanded = nextPayloadOfType(participant, "room_disbanded");
  const creatorClosed = once(creator, "close");
  const participantClosed = once(participant, "close");
  creator.send(JSON.stringify({ type: "disband_room", commandLetters }));
  assert.deepEqual(await Promise.all([creatorDisbanded, participantDisbanded]), [
    { type: "room_disbanded" },
    { type: "room_disbanded" },
  ]);
  await Promise.all([creatorClosed, participantClosed]);

  const recreatedRoom = await createRoom();
  assert.equal(recreatedRoom.code, roomCode);
  const { socket: newParticipant, firstPayload: newInitPromise } = await connectWebSocket(
    "192.0.2.10",
    recreatedRoom.code,
  );
  t.after(() => newParticipant.close());
  const newInit = await newInitPromise;
  assert.deepEqual(newInit.messages, []);
});

test("assigns room codes from 01 to 50 and rejects room creation above capacity", async () => {
  for (let roomNumber = 1; roomNumber <= 50; roomNumber += 1) {
    const response = await fetch(`${baseUrl}/api/rooms`, {
      method: "POST",
      headers: { "X-Owner-Token": randomUUID() },
    });
    assert.equal(response.status, 201);
    const room = await response.json();
    assert.equal(room.code, String(roomNumber).padStart(2, "0"));
    assert.match(room.commandLetters, /^[A-Z]{2}$/);
  }

  const fullResponse = await fetch(`${baseUrl}/api/rooms`, {
    method: "POST",
    headers: { "X-Owner-Token": randomUUID() },
  });
  assert.equal(fullResponse.status, 409);
  assert.match((await fullResponse.json()).error, /作成できる部屋がありません/);
});

test("keeps participants, messages, and participant counts isolated by room code", async (t) => {
  const {
    socket: firstRoomClient,
    firstPayload: firstRoomInitPromise,
    roomCode: firstRoomCode,
    commandLetters: firstRoomCommandLetters,
  } = await connectWebSocket("203.0.113.42");
  const {
    socket: secondRoomClient,
    firstPayload: secondRoomInitPromise,
    roomCode: secondRoomCode,
  } = await connectWebSocket("198.51.100.15");
  t.after(() => { firstRoomClient.close(); secondRoomClient.close(); });

  const [firstRoomInit, secondRoomInit] = await Promise.all([firstRoomInitPromise, secondRoomInitPromise]);
  assert.equal(firstRoomInit.participants, 1);
  assert.equal(secondRoomInit.participants, 1);
  assert.deepEqual(firstRoomInit.messages, []);
  assert.deepEqual(secondRoomInit.messages, []);

  const messagesForFirstRoom = nextPayloadOfType(firstRoomClient, "message");
  firstRoomClient.send(JSON.stringify({
    type: "message",
    name: "参加者1",
    text: "room one only",
    color: "#fffafa",
  }));
  assert.equal((await messagesForFirstRoom).text, "room one only");

  const firstRoomLock = nextPayloadOfType(firstRoomClient, "lock_state");
  firstRoomClient.send(JSON.stringify({ type: "admin_lock", commandLetters: firstRoomCommandLetters }));
  const firstRoomLockState = await firstRoomLock;
  assert.equal(firstRoomLockState.locked, false);
  assert.equal(firstRoomLockState.isLockOwner, true);

  const unexpectedMessages = [];
  firstRoomClient.on("message", (data) => {
    const payload = JSON.parse(data.toString());
    if (payload.type === "message") {
      unexpectedMessages.push(payload.text);
    }
  });
  const messagesForSecondRoom = nextPayloadOfType(secondRoomClient, "message");
  secondRoomClient.send(JSON.stringify({
    type: "message",
    name: "参加者2",
    text: "room two only",
    color: "#fffafa",
  }));
  assert.equal((await messagesForSecondRoom).text, "room two only");
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.deepEqual(unexpectedMessages, []);

  const { socket: returningClient, firstPayload: returningInitPromise } = await connectWebSocket(
    "192.0.2.10",
    secondRoomCode,
  );
  t.after(() => returningClient.close());
  const returningInit = await returningInitPromise;
  assert.deepEqual(returningInit.messages.map((message) => message.text), ["room two only"]);
  assert.equal(returningInit.locked, false);

  const { socket: lockedRoomClient, firstPayload: lockedRoomInitPromise } = await connectWebSocket(
    "192.0.2.11",
    firstRoomCode,
  );
  t.after(() => lockedRoomClient.close());
  const lockedRoomInit = await lockedRoomInitPromise;
  assert.deepEqual(lockedRoomInit.messages.map((message) => message.text), ["room one only"]);
  assert.equal(lockedRoomInit.locked, true);
});

test("uploads an image, shares it in chat, and serves byte ranges", async (t) => {
  const { socket, firstPayload, ownerToken, roomCode } = await connectWebSocket("203.0.113.42");
  t.after(() => socket.close());
  await firstPayload;

  const imageBytes = Buffer.from("sample-image-content");
  const uploadResponse = await fetch(`${baseUrl}/api/uploads?name=sample.png&room=${roomCode}`, {
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

test("only the creator can use the generated lock command", async (t) => {
  const {
    socket: first,
    firstPayload: firstInitPromise,
    roomCode,
    commandLetters,
  } = await connectWebSocket("203.0.113.42");
  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket("198.51.100.15", roomCode);
  t.after(() => { first.close(); second.close(); });
  await Promise.all([firstInitPromise, secondInitPromise]);

  second.send(JSON.stringify({ type: "admin_lock", commandLetters }));
  await new Promise((resolve) => setTimeout(resolve, 20));
  const { socket: observer, firstPayload: observerInitPromise } = await connectWebSocket("192.0.2.12", roomCode);
  t.after(() => observer.close());
  const observerInit = await observerInitPromise;
  assert.equal(observerInit.locked, false);

  const firstLock = nextPayloadOfType(first, "lock_state");
  const secondLock = nextPayloadOfType(second, "lock_state");
  first.send(JSON.stringify({ type: "admin_lock", commandLetters }));

  const firstLockState = await firstLock;
  const secondLockState = await secondLock;
  assert.equal(firstLockState.locked, false);
  assert.equal(firstLockState.isLockOwner, true);
  assert.equal(secondLockState.locked, true);
  assert.equal(secondLockState.isLockOwner, false);
});

test("only the creator can reset history using the generated command", async (t) => {
  const {
    socket: first,
    firstPayload: firstInitPromise,
    roomCode,
    commandLetters,
  } = await connectWebSocket("203.0.113.42");
  const { socket: second, firstPayload: secondInitPromise } = await connectWebSocket("198.51.100.15", roomCode);
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
  first.send(JSON.stringify({ type: "admin_lock", commandLetters }));
  const firstLockState = await firstLock;
  const secondLockState = await secondLock;
  assert.equal(firstLockState.type, "lock_state");
  assert.equal(firstLockState.locked, false);
  assert.equal(firstLockState.isLockOwner, true);
  assert.equal(secondLockState.type, "lock_state");
  assert.equal(secondLockState.locked, true);
  assert.equal(secondLockState.isLockOwner, false);

  second.send(JSON.stringify({ type: "reset_chat", commandLetters }));
  await new Promise((resolve) => setTimeout(resolve, 20));
  const { socket: observer, firstPayload: observerInitPromise } = await connectWebSocket("192.0.2.13", roomCode);
  t.after(() => observer.close());
  const observerInit = await observerInitPromise;
  assert.equal(observerInit.messages.length, 1);
  assert.equal(observerInit.locked, true);

  await Promise.all([
    nextPayloadOfType(first, "participants"),
    nextPayloadOfType(second, "participants"),
  ]);
  const firstReset = nextPayloadOfType(first, "messages_cleared");
  const secondReset = nextPayloadOfType(second, "messages_cleared");
  const firstResetLock = nextPayloadOfType(first, "lock_state");
  const secondResetLock = nextPayloadOfType(second, "lock_state");
  first.send(JSON.stringify({ type: "reset_chat", commandLetters }));
  assert.deepEqual(await firstReset, { type: "messages_cleared" });
  assert.deepEqual(await secondReset, { type: "messages_cleared" });
  const firstResetLockState = await firstResetLock;
  const secondResetLockState = await secondResetLock;
  assert.equal(firstResetLockState.type, "lock_state");
  assert.equal(firstResetLockState.locked, false);
  assert.equal(secondResetLockState.type, "lock_state");
  assert.equal(secondResetLockState.locked, false);
});
