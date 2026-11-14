const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
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
const history = [];
const uploadedMedia = new Map();
let isLocked = false;
let lockOwnerToken = null;

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

function clearHistory() {
  for (const message of history) {
    removeMediaFile(message.media);
  }
  for (const media of uploadedMedia.values()) {
    removeMediaFile(media);
  }
  history.length = 0;
  uploadedMedia.clear();
}

async function handleUpload(request, response, requestUrl) {
  const contentType = request.headers["content-type"]?.split(";")[0].trim().toLowerCase();
  const mediaType = MEDIA_TYPES.get(contentType);
  const ownerToken = request.headers["x-owner-token"];
  if (
    !mediaType
    || typeof ownerToken !== "string"
    || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(ownerToken)
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
    uploadedMedia.set(id, { ...media, ownerToken });
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

  if (request.method === "OPTIONS" && pathname === "/api/uploads") {
    setCorsHeaders(response);
    response.writeHead(204).end();
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

function broadcast(payload) {
  const serialized = JSON.stringify(payload);
  for (const client of webSocketServer.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(serialized);
    }
  }
}

function broadcastLockState() {
  for (const client of webSocketServer.clients) {
    if (client.readyState !== WebSocket.OPEN) {
      continue;
    }

    const isLockOwner = Boolean(client.ownerToken) && client.ownerToken === lockOwnerToken;
    client.send(JSON.stringify({
      type: "lock_state",
      locked: isLocked && !isLockOwner,
      isLockOwner,
      lockOwnerToken,
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

function broadcastMessage(message) {
  for (const client of webSocketServer.clients) {
    if (client.readyState === WebSocket.OPEN && client.ownerToken) {
      client.send(JSON.stringify(presentMessage(message, client.ownerToken)));
    }
  }
}

function broadcastReactions(message) {
  for (const client of webSocketServer.clients) {
    if (client.readyState === WebSocket.OPEN && client.ownerToken) {
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
  const isAllowedAdmin = true;

  let adminAttemptWindowStartedAt = Date.now();
  let adminAttemptsInWindow = 0;

  socket.on("message", (rawMessage) => {
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
        messages: history.map((message) => presentMessage(message, socket.ownerToken)),
        participants: webSocketServer.clients.size,
        locked: isLocked && socket.ownerToken !== lockOwnerToken,
        isLockOwner: Boolean(socket.ownerToken) && socket.ownerToken === lockOwnerToken,
        lockOwnerToken,
        canDeleteAll: isAllowedAdmin,
      }));
      broadcast({ type: "participants", count: webSocketServer.clients.size });
      return;
    }

    if (!socket.ownerToken) {
      return;
    }

    if (payload.type === "admin_lock") {
      const now = Date.now();
      if (now - adminAttemptWindowStartedAt >= ADMIN_ATTEMPT_WINDOW_MS) {
        adminAttemptWindowStartedAt = now;
        adminAttemptsInWindow = 0;
      }

      if (adminAttemptsInWindow >= MAX_ADMIN_ATTEMPTS_PER_WINDOW) {
        return;
      }

      adminAttemptsInWindow += 1;
      isLocked = !isLocked;
      lockOwnerToken = isLocked ? socket.ownerToken : null;
      broadcastLockState();
      return;
    }

    if (payload.type === "delete_all_messages") {
      clearHistory();
      broadcast({ type: "messages_cleared" });
      return;
    }

    if (payload.type === "reset_chat") {
      clearHistory();
      isLocked = false;
      lockOwnerToken = null;
      broadcast({ type: "messages_cleared" });
      broadcastLockState();
      return;
    }

    if (payload.type === "delete_message") {
      const messageIndex = history.findIndex(
        (message) => message.id === payload.id && message.ownerToken === socket.ownerToken,
      );
      if (messageIndex === -1) {
        return;
      }

      const [deletedMessage] = history.splice(messageIndex, 1);
      removeMediaFile(deletedMessage.media);
      broadcast({ type: "message_deleted", id: payload.id });
      return;
    }

    if (payload.type === "reaction") {
      if (typeof payload.id !== "string" || !REACTIONS.has(payload.reaction)) {
        return;
      }

      const message = history.find((item) => item.id === payload.id);
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
      broadcastReactions(message);
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
      || (pendingMedia && pendingMedia.ownerToken !== socket.ownerToken)
    ) {
      return;
    }

    const name = typeof payload.name === "string"
      ? payload.name.trim().slice(0, MAX_NAME_LENGTH)
      : "";
    const replyTo = typeof payload.replyToId === "string"
      ? history.find((item) => item.id === payload.replyToId)
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

    history.push(message);
    if (history.length > MAX_HISTORY) {
      const removedMessage = history.shift();
      removeMediaFile(removedMessage.media);
    }
    broadcastMessage(message);
  });

  socket.on("close", () => {
    broadcast({ type: "participants", count: webSocketServer.clients.size });
  });
});

server.listen(PORT, () => {
  const address = server.address();
  console.log(`Chat is running at http://localhost:${address.port}`);
});
