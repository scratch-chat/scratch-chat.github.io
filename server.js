const http = require("node:http");
const fs = require("node:fs");
const net = require("node:net");
const path = require("node:path");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT === undefined ? 8000 : Number(process.env.PORT);
const PUBLIC_DIR = path.join(__dirname, "public");
const MAX_HISTORY = 100;
const MAX_MESSAGE_LENGTH = 500;
const MAX_NAME_LENGTH = 24;
const ADMIN_ATTEMPT_WINDOW_MS = 60_000;
const MAX_ADMIN_ATTEMPTS_PER_WINDOW = 5;
const ADMIN_ALLOWED_IP = process.env.ADMIN_ALLOWED_IP?.trim() || "";
const COLORS = new Set(["#fffafa", "#fffbf5", "#f8fcf8", "#f7faff", "#fbf9ff"]);
const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".svg": "image/svg+xml",
};
const history = [];
let isLocked = false;

if (ADMIN_ALLOWED_IP && net.isIP(ADMIN_ALLOWED_IP) === 0) {
  throw new Error("ADMIN_ALLOWED_IP must be a valid IP address.");
}

const server = http.createServer((request, response) => {
  const pathname = new URL(request.url, `http://${request.headers.host}`).pathname;
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

function getClientIp(request) {
  const forwardedFor = request.headers["x-forwarded-for"];
  const clientIp = typeof forwardedFor === "string"
    ? forwardedFor.split(",")[0].trim()
    : request.socket.remoteAddress || "";
  return clientIp.startsWith("::ffff:") ? clientIp.slice(7) : clientIp;
}

webSocketServer.on("connection", (socket, request) => {
  const isAllowedAdmin = ADMIN_ALLOWED_IP !== ""
    && getClientIp(request) === ADMIN_ALLOWED_IP;

  socket.send(JSON.stringify({
    type: "init",
    messages: history,
    participants: webSocketServer.clients.size,
    locked: isLocked,
  }));
  broadcast({ type: "participants", count: webSocketServer.clients.size });

  let adminAttemptWindowStartedAt = Date.now();
  let adminAttemptsInWindow = 0;

  socket.on("message", (rawMessage) => {
    let payload;
    try {
      payload = JSON.parse(rawMessage.toString());
    } catch {
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
      if (!isAllowedAdmin) {
        return;
      }

      isLocked = !isLocked;
      broadcast({ type: "lock_state", locked: isLocked });
      return;
    }

    if (payload.type !== "message" || typeof payload.text !== "string") {
      return;
    }

    const text = payload.text.trim().slice(0, MAX_MESSAGE_LENGTH);
    if (!text) {
      return;
    }

    const name = typeof payload.name === "string"
      ? payload.name.trim().slice(0, MAX_NAME_LENGTH)
      : "";
    const message = {
      type: "message",
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      name: name || "ゲスト",
      text,
      color: COLORS.has(payload.color) ? payload.color : "#f7faff",
      timestamp: Date.now(),
    };

    history.push(message);
    if (history.length > MAX_HISTORY) {
      history.shift();
    }
    broadcast(message);
  });

  socket.on("close", () => {
    broadcast({ type: "participants", count: webSocketServer.clients.size });
  });
});

server.listen(PORT, () => {
  const address = server.address();
  console.log(`Chat is running at http://localhost:${address.port}`);
});
