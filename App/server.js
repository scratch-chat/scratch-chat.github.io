const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { randomBytes, randomUUID } = require("node:crypto");
const { Transform } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const { WebSocketServer, WebSocket } = require("./vendor/ws.cjs");

const PORT = process.env.PORT === undefined ? 8000 : Number(process.env.PORT);
const PUBLIC_DIR = path.join(__dirname, "public");
const UPLOADS_DIR = path.join(__dirname, "uploads");
fs.mkdirSync(UPLOADS_DIR, { recursive: true });
const MAX_HISTORY = 100;
const MAX_MESSAGE_LENGTH = 500;
const MAX_NAME_LENGTH = 24;
const MAX_IMAGE_SIZE = 20 * 1024 * 1024;
const MAX_VIDEO_SIZE = 300 * 1024 * 1024;
const MAX_VIDEO_DURATION_SECONDS = 15 * 60;
const ADMIN_ATTEMPT_WINDOW_MS = 60_000;
const MAX_ADMIN_ATTEMPTS_PER_WINDOW = 5;
const DEFAULT_ROOM_CODE = "01";
const ROOM_CODE_COUNT = 50;
const ROOM_EMPTY_GRACE_MS = 5 * 60 * 1000;
const DEFAULT_ROOM_INACTIVITY_TIMEOUT_MS = 24 * 60 * 60 * 1000;
const ROOM_INACTIVITY_TIMEOUT_MS = process.env.ROOM_INACTIVITY_TIMEOUT_MS === undefined
  ? DEFAULT_ROOM_INACTIVITY_TIMEOUT_MS
  : Number(process.env.ROOM_INACTIVITY_TIMEOUT_MS);
if (!Number.isSafeInteger(ROOM_INACTIVITY_TIMEOUT_MS) || ROOM_INACTIVITY_TIMEOUT_MS <= 0) {
  throw new RangeError("ROOM_INACTIVITY_TIMEOUT_MS must be a positive integer.");
}
const ROOM_CODES = Array.from({ length: ROOM_CODE_COUNT }, (_, index) => String(index + 1).padStart(2, "0"));
const ROOM_CODE_PATTERN = /^(0[1-9]|[1-4][0-9]|50)$/;
const OWNER_TOKEN_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const ADMIN_ALLOWED_IP = process.env.ADMIN_ALLOWED_IP?.trim() || "";
const COLORS = new Set(["#fffafa", "#fffbf5", "#f8fcf8", "#f7faff", "#fbf9ff"]);
const REACTIONS = new Set(["heart", "star"]);
const MEDIA_TYPES = new Map([
  ["image/jpeg", { category: "image", extension: "jpg" }],
  ["image/png", { category: "image", extension: "png" }],
  ["image/gif", { category: "image", extension: "gif" }],
  ["image/webp", { category: "image", extension: "webp" }],
  ["image/avif", { category: "image", extension: "avif" }],
  ["video/mp4", { category: "video", extension: "mp4" }],
  ["video/webm", { category: "video", extension: "webm" }],
  ["video/quicktime", { category: "video", extension: "mov" }],
]);
const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".svg": "image/svg+xml",
};
const rooms = new Map();
const uploadedMedia = new Map();

function getRoom(roomCode) {
  let room = rooms.get(roomCode);
  if (!room) {
    room = {
      history: [],
      isLocked: false,
      lockOwnerToken: null,
      creatorToken: null,
      commandLetters: null,
      emptyTimeout: null,
      inactivityTimeout: null,
    };
    rooms.set(roomCode, room);
  } else if (room.emptyTimeout) {
    clearTimeout(room.emptyTimeout);
    room.emptyTimeout = null;
  }
  return room;
}

function createCommandLetters() {
  const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  return [...randomBytes(2)].map((value) => letters[value % letters.length]).join("");
}

function isValidRoomCommand(socket, payload, room) {
  return socket.ownerToken === room.creatorToken && payload.commandLetters === room.commandLetters;
}

function getParticipantCount(roomCode) {
  return [...webSocketServer.clients].filter((client) => client.roomCode === roomCode).length;
}

function setCorsHeaders(response) {
  response.setHeader("Access-Control-Allow-Origin", "*");
  response.setHeader("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
  response.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Media-Duration, X-Owner-Token");
}

function respondWithJson(response, statusCode, payload) {
  setCorsHeaders(response);
  response.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload));
}

function mediaFilePath(media) {
  if (!media || typeof media.url !== "string") {
    return null;
  }
  const match = media.url.match(/^\/media\/([0-9a-f-]{36})\.([a-z0-9]+)$/i);
  if (!match) {
    return null;
  }
  return path.join(UPLOADS_DIR, `${match[1]}.${match[2]}`);
}

function removeMediaFile(media) {
  const filePath = mediaFilePath(media);
  if (!filePath) {
    return;
  }
  fs.promises.unlink(filePath).catch((error) => {
    if (error.code !== "ENOENT") {
      console.error("Could not remove uploaded media:", error);
    }
  });
}

function clearHistory(roomCode, room) {
  for (const message of room.history) {
    removeMediaFile(message.media);
  }
  room.history.length = 0;
  for (const [mediaId, media] of uploadedMedia) {
    if (media.roomCode === roomCode) {
      removeMediaFile(media);
      uploadedMedia.delete(mediaId);
    }
  }
}

function scheduleRoomRelease(roomCode, room) {
  if (room.emptyTimeout) {
    clearTimeout(room.emptyTimeout);
  }
  room.emptyTimeout = setTimeout(() => {
    if (getParticipantCount(roomCode) !== 0 || rooms.get(roomCode) !== room) {
      return;
    }

    releaseRoom(roomCode, room);
  }, ROOM_EMPTY_GRACE_MS);
}

function scheduleRoomExpiry(roomCode, room) {
  if (room.inactivityTimeout) {
    clearTimeout(room.inactivityTimeout);
  }
  room.inactivityTimeout = setTimeout(() => {
    if (rooms.get(roomCode) === room) {
      disbandRoom(roomCode, room);
    }
  }, ROOM_INACTIVITY_TIMEOUT_MS);
}

function releaseRoom(roomCode, room) {
  if (room.emptyTimeout) {
    clearTimeout(room.emptyTimeout);
    room.emptyTimeout = null;
  }
  if (room.inactivityTimeout) {
    clearTimeout(room.inactivityTimeout);
    room.inactivityTimeout = null;
  }
  clearHistory(roomCode, room);
  if (rooms.get(roomCode) === room) {
    rooms.delete(roomCode);
  }
}

async function handleUpload(request, response, requestUrl) {
  const contentType = request.headers["content-type"]?.split(";")[0].trim().toLowerCase();
  const mediaType = MEDIA_TYPES.get(contentType);
  const ownerToken = request.headers["x-owner-token"];
  const roomCode = requestUrl.searchParams.get("room") || DEFAULT_ROOM_CODE;
  if (
    !mediaType
    || !ROOM_CODE_PATTERN.test(roomCode)
    || typeof ownerToken !== "string"
    || !OWNER_TOKEN_PATTERN.test(ownerToken)
  ) {
    request.resume();
    respondWithJson(response, 415, { error: "対応していないファイル形式です。" });
    return;
  }

  const maxSize = mediaType.category === "image" ? MAX_IMAGE_SIZE : MAX_VIDEO_SIZE;
  const duration = Number(request.headers["x-media-duration"]);
  if (mediaType.category === "video" && (!Number.isFinite(duration) || duration <= 0 || duration > MAX_VIDEO_DURATION_SECONDS)) {
    request.resume();
    respondWithJson(response, 400, { error: "動画は15分以内にしてください。" });
    return;
  }
  if (Number(request.headers["content-length"]) > maxSize) {
    request.resume();
    respondWithJson(response, 413, { error: "ファイルサイズが上限を超えています。" });
    return;
  }

  const name = (requestUrl.searchParams.get("name") || "media")
    .replace(/[\\/\0-\x1f\x7f]/g, "_")
    .slice(0, 255);
  const id = randomUUID();
  const url = `/media/${id}.${mediaType.extension}`;
  const filePath = path.join(UPLOADS_DIR, `${id}.${mediaType.extension}`);
  let size = 0;
  const sizeLimit = new Transform({
    transform(chunk, encoding, callback) {
      size += chunk.length;
      if (size > maxSize) {
        const error = new Error("Upload exceeds the allowed size");
        error.code = "UPLOAD_TOO_LARGE";
        callback(error);
        return;
      }
      callback(null, chunk);
    },
  });

  try {
    await pipeline(request, sizeLimit, fs.createWriteStream(filePath, { flags: "wx" }));
    const media = {
      id,
      url,
      type: mediaType.category,
      mimeType: contentType,
      name: name || "media",
      size,
      duration: mediaType.category === "video" ? duration : null,
    };
    uploadedMedia.set(id, { ...media, ownerToken, roomCode });
    respondWithJson(response, 201, media);
  } catch (error) {
    fs.promises.unlink(filePath).catch((unlinkError) => {
      if (unlinkError.code !== "ENOENT") {
        console.error("Could not remove incomplete upload:", unlinkError);
      }
    });
    if (error.code === "UPLOAD_TOO_LARGE") {
      respondWithJson(response, 413, { error: "ファイルサイズが上限を超えています。" });
    } else if (!request.destroyed) {
      console.error("Could not save uploaded media:", error);
      respondWithJson(response, 500, { error: "ファイルを保存できませんでした。" });
    }
  }
}

async function serveMedia(request, response, pathname) {
  const match = pathname.match(/^\/media\/([0-9a-f-]{36})\.([a-z0-9]+)$/i);
  if (!match || ![...MEDIA_TYPES.values()].some(({ extension }) => extension === match[2])) {
    response.writeHead(404).end("Not found");
    return;
  }
  const filePath = path.join(UPLOADS_DIR, `${match[1]}.${match[2]}`);
  let stat;
  try {
    stat = await fs.promises.stat(filePath);
  } catch (error) {
    if (error.code === "ENOENT") {
      response.writeHead(404).end("Not found");
      return;
    }
    throw error;
  }

  const range = request.headers.range?.match(/^bytes=(\d*)-(\d*)$/);
  let start = 0;
  let end = stat.size - 1;
  let statusCode = 200;
  if (request.headers.range) {
    if (!range || (!range[1] && !range[2])) {
      response.writeHead(416, { "Content-Range": `bytes */${stat.size}` }).end();
      return;
    }
    if (range[1]) {
      start = Number(range[1]);
      end = range[2] ? Number(range[2]) : end;
    } else {
      const suffixLength = Number(range[2]);
      start = Math.max(0, stat.size - suffixLength);
    }
    if (start >= stat.size || end < start) {
      response.writeHead(416, { "Content-Range": `bytes */${stat.size}` }).end();
      return;
    }
    end = Math.min(end, stat.size - 1);
    statusCode = 206;
  }

  const extension = `.${match[2]}`;
  const mimeType = [...MEDIA_TYPES].find(([, value]) => `.${value.extension}` === extension)?.[0]
    || "application/octet-stream";
  response.writeHead(statusCode, {
    "Accept-Ranges": "bytes",
    "Cache-Control": "public, max-age=31536000, immutable",
    "Content-Length": end - start + 1,
    "Content-Type": mimeType,
    "Content-Disposition": "inline",
    "X-Content-Type-Options": "nosniff",
    ...(statusCode === 206 ? { "Content-Range": `bytes ${start}-${end}/${stat.size}` } : {}),
  });
  if (request.method === "HEAD") {
    response.end();
    return;
  }
  await pipeline(fs.createReadStream(filePath, { start, end }), response);
}

const server = http.createServer(async (request, response) => {
  const requestUrl = new URL(request.url, `http://${request.headers.host}`);
  const pathname = requestUrl.pathname;

  if (request.method === "OPTIONS" && ["/api/uploads", "/api/rooms"].includes(pathname)) {
    setCorsHeaders(response);
    response.writeHead(204).end();
    return;
  }
  if (request.method === "POST" && pathname === "/api/rooms") {
    const creatorToken = request.headers["x-owner-token"];
    if (typeof creatorToken !== "string" || !OWNER_TOKEN_PATTERN.test(creatorToken)) {
      respondWithJson(response, 400, { error: "部屋を作成できませんでした。" });
      return;
    }

    const roomCode = ROOM_CODES.find((code) => !rooms.has(code));
    if (!roomCode) {
      respondWithJson(response, 409, { error: "作成できる部屋がありません。しばらくしてからもう一度お試しください。" });
      return;
    }

    const room = getRoom(roomCode);
    room.creatorToken = creatorToken;
    room.commandLetters = createCommandLetters();
    scheduleRoomRelease(roomCode, room);
    scheduleRoomExpiry(roomCode, room);
    respondWithJson(response, 201, { code: roomCode, commandLetters: room.commandLetters });
    return;
  }
  if (request.method === "POST" && pathname === "/api/uploads") {
    await handleUpload(request, response, requestUrl);
    return;
  }
  if ((request.method === "GET" || request.method === "HEAD") && pathname.startsWith("/media/")) {
    try {
      await serveMedia(request, response, pathname);
    } catch (error) {
      if (!response.destroyed) {
        console.error("Could not serve uploaded media:", error);
        response.destroy(error);
      }
    }
    return;
  }

  const requestedPath = pathname === "/" ? "/index.html" : pathname;
  const filePath = path.resolve(PUBLIC_DIR, `.${requestedPath}`);

  if (!filePath.startsWith(`${PUBLIC_DIR}${path.sep}`)) {
    response.writeHead(403).end("Forbidden");
    return;
  }

  fs.readFile(filePath, (error, content) => {
    if (error) {
      response.writeHead(error.code === "ENOENT" ? 404 : 500).end("Not found");
      return;
    }

    response.writeHead(200, {
      "Content-Type": MIME_TYPES[path.extname(filePath)] || "application/octet-stream",
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    });
    response.end(content);
  });
});

const webSocketServer = new WebSocketServer({ server, path: "/ws" });

function broadcastToRoom(roomCode, payload) {
  const serialized = JSON.stringify(payload);
  for (const client of webSocketServer.clients) {
    if (client.readyState === WebSocket.OPEN && client.roomCode === roomCode) {
      client.send(serialized);
    }
  }
}

function disbandRoom(roomCode, room) {
  releaseRoom(roomCode, room);

  const serialized = JSON.stringify({ type: "room_disbanded" });
  for (const client of webSocketServer.clients) {
    if (client.roomCode !== roomCode) {
      continue;
    }

    if (client.readyState === WebSocket.OPEN) {
      client.send(serialized);
    }
    client.roomCode = null;
    client.close(1000, "Room disbanded");
  }
}

function broadcastLockState(roomCode, room) {
  for (const client of webSocketServer.clients) {
    if (client.readyState !== WebSocket.OPEN || client.roomCode !== roomCode) {
      continue;
    }

    const isLockOwner = Boolean(client.ownerToken) && client.ownerToken === room.lockOwnerToken;
    client.send(JSON.stringify({
      type: "lock_state",
      locked: room.isLocked && !isLockOwner,
      isLockOwner,
      lockOwnerToken: room.lockOwnerToken,
    }));
  }
}

function presentMessage(message, ownerToken) {
  const {
    ownerToken: messageOwnerToken,
    reactions: reactionOwners,
    ...publicMessage
  } = message;
  return {
    ...publicMessage,
    canDelete: messageOwnerToken === ownerToken,
    reactions: {
      heart: reactionOwners.heart.length,
      star: reactionOwners.star.length,
    },
    myReactions: {
      heart: reactionOwners.heart.includes(ownerToken),
      star: reactionOwners.star.includes(ownerToken),
    },
  };
}

function broadcastMessage(roomCode, message) {
  for (const client of webSocketServer.clients) {
    if (client.readyState === WebSocket.OPEN && client.roomCode === roomCode && client.ownerToken) {
      client.send(JSON.stringify(presentMessage(message, client.ownerToken)));
    }
  }
}

function broadcastReactions(roomCode, message) {
  for (const client of webSocketServer.clients) {
    if (client.readyState === WebSocket.OPEN && client.roomCode === roomCode && client.ownerToken) {
      const presentedMessage = presentMessage(message, client.ownerToken);
      client.send(JSON.stringify({
        type: "reaction_update",
        id: message.id,
        reactions: presentedMessage.reactions,
        myReactions: presentedMessage.myReactions,
      }));
    }
  }
}

function getClientIp(request) {
  const forwardedFor = request.headers["x-forwarded-for"];
  const clientIp = typeof forwardedFor === "string"
    ? forwardedFor.split(",")[0].trim()
    : request.socket.remoteAddress || "";
  return clientIp.startsWith("::ffff:") ? clientIp.slice(7) : clientIp;
}

webSocketServer.on("connection", (socket, request) => {
  const requestUrl = new URL(request.url, `http://${request.headers.host}`);
  const roomCode = requestUrl.searchParams.get("room") || DEFAULT_ROOM_CODE;
  if (!ROOM_CODE_PATTERN.test(roomCode)) {
    socket.close(1008, "Invalid room code");
    return;
  }
  if (!rooms.has(roomCode)) {
    socket.send(JSON.stringify({ type: "room_not_found" }));
    socket.close(1008, "Room not found");
    return;
  }
  socket.roomCode = roomCode;
  const room = getRoom(roomCode);

  let adminAttemptWindowStartedAt = Date.now();
  let adminAttemptsInWindow = 0;

  socket.on("message", (rawMessage) => {
    if (rooms.get(roomCode) !== room) {
      return;
    }

    let payload;
    try {
      payload = JSON.parse(rawMessage.toString());
    } catch {
      return;
    }

    if (payload.type === "identify") {
      if (
        socket.ownerToken
        || typeof payload.token !== "string"
        || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(payload.token)
      ) {
        return;
      }

      socket.ownerToken = payload.token;
      socket.send(JSON.stringify({
        type: "init",
        messages: room.history.map((message) => presentMessage(message, socket.ownerToken)),
        participants: getParticipantCount(roomCode),
        locked: room.isLocked && socket.ownerToken !== room.lockOwnerToken,
        isLockOwner: Boolean(socket.ownerToken) && socket.ownerToken === room.lockOwnerToken,
        lockOwnerToken: room.lockOwnerToken,
        canDeleteAll: socket.ownerToken === room.creatorToken,
      }));
      broadcastToRoom(roomCode, { type: "participants", count: getParticipantCount(roomCode) });
      return;
    }

    if (!socket.ownerToken) {
      return;
    }

    if (payload.type === "disband_room") {
      if (!isValidRoomCommand(socket, payload, room)) {
        return;
      }
      disbandRoom(roomCode, room);
      return;
    }

    if (payload.type === "admin_lock") {
      if (!isValidRoomCommand(socket, payload, room)) {
        return;
      }

      const now = Date.now();
      if (now - adminAttemptWindowStartedAt >= ADMIN_ATTEMPT_WINDOW_MS) {
        adminAttemptWindowStartedAt = now;
        adminAttemptsInWindow = 0;
      }

      if (adminAttemptsInWindow >= MAX_ADMIN_ATTEMPTS_PER_WINDOW) {
        return;
      }

      adminAttemptsInWindow += 1;
      room.isLocked = !room.isLocked;
      room.lockOwnerToken = room.isLocked ? socket.ownerToken : null;
      broadcastLockState(roomCode, room);
      return;
    }

    if (payload.type === "delete_all_messages") {
      if (!isValidRoomCommand(socket, payload, room)) {
        return;
      }
      clearHistory(roomCode, room);
      broadcastToRoom(roomCode, { type: "messages_cleared" });
      return;
    }

    if (payload.type === "reset_chat") {
      if (!isValidRoomCommand(socket, payload, room)) {
        return;
      }
      clearHistory(roomCode, room);
      room.isLocked = false;
      room.lockOwnerToken = null;
      broadcastToRoom(roomCode, { type: "messages_cleared" });
      broadcastLockState(roomCode, room);
      return;
    }

    if (payload.type === "delete_message") {
      const messageIndex = room.history.findIndex(
        (message) => message.id === payload.id && message.ownerToken === socket.ownerToken,
      );
      if (messageIndex === -1) {
        return;
      }

      const [deletedMessage] = room.history.splice(messageIndex, 1);
      removeMediaFile(deletedMessage.media);
      broadcastToRoom(roomCode, { type: "message_deleted", id: payload.id });
      return;
    }

    if (payload.type === "reaction") {
      if (typeof payload.id !== "string" || !REACTIONS.has(payload.reaction)) {
        return;
      }

      const message = room.history.find((item) => item.id === payload.id);
      if (!message) {
        return;
      }

      const owners = message.reactions[payload.reaction];
      const ownerIndex = owners.indexOf(socket.ownerToken);
      if (ownerIndex === -1) {
        owners.push(socket.ownerToken);
      } else {
        owners.splice(ownerIndex, 1);
      }
      broadcastReactions(roomCode, message);
      return;
    }

    if (
      payload.type !== "message"
      || (payload.text !== undefined && typeof payload.text !== "string")
    ) {
      return;
    }

    const text = typeof payload.text === "string" ? payload.text.trim().slice(0, MAX_MESSAGE_LENGTH) : "";
    const pendingMedia = typeof payload.mediaId === "string" ? uploadedMedia.get(payload.mediaId) : null;
    if (
      (!text && !pendingMedia)
      || (pendingMedia && (pendingMedia.ownerToken !== socket.ownerToken || pendingMedia.roomCode !== roomCode))
    ) {
      return;
    }

    const name = typeof payload.name === "string"
      ? payload.name.trim().slice(0, MAX_NAME_LENGTH)
      : "";
    const replyTo = typeof payload.replyToId === "string"
      ? room.history.find((item) => item.id === payload.replyToId)
      : undefined;
    const message = {
      type: "message",
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      name: name || "ゲスト",
      text,
      color: COLORS.has(payload.color) ? payload.color : "#f7faff",
      timestamp: Date.now(),
      ownerToken: socket.ownerToken,
      media: pendingMedia
        ? {
            id: pendingMedia.id,
            url: pendingMedia.url,
            type: pendingMedia.type,
            mimeType: pendingMedia.mimeType,
            name: pendingMedia.name,
            size: pendingMedia.size,
            duration: pendingMedia.duration,
          }
        : null,
      reply: replyTo
        ? { id: replyTo.id, name: replyTo.name, text: replyTo.text }
        : null,
      reactions: { heart: [], star: [] },
    };
    if (pendingMedia) {
      uploadedMedia.delete(pendingMedia.id);
    }

    room.history.push(message);
    if (room.history.length > MAX_HISTORY) {
      const removedMessage = room.history.shift();
      removeMediaFile(removedMessage.media);
    }
    scheduleRoomExpiry(roomCode, room);
    broadcastMessage(roomCode, message);
  });

  socket.on("close", () => {
    if (rooms.get(roomCode) !== room) {
      return;
    }
    broadcastToRoom(roomCode, { type: "participants", count: getParticipantCount(roomCode) });
    if (getParticipantCount(roomCode) === 0) {
      releaseRoom(roomCode, room);
    }
  });
});

server.listen(PORT, () => {
  const address = server.address();
  console.log(`Chat is running at http://localhost:${address.port}`);
});
