const COLORS = [
  { value: "#fffafa", swatch: "#ffd6d6", label: "赤" },
  { value: "#fffbf5", swatch: "#ffe2b8", label: "オレンジ" },
  { value: "#f8fcf8", swatch: "#d6f2df", label: "緑" },
  { value: "#f7faff", swatch: "#d6e8ff", label: "青" },
  { value: "#fbf9ff", swatch: "#e8dcff", label: "紫" },
];

const messagesElement = document.querySelector("#messages");
const emptyState = document.querySelector("#empty-state");
const colorOptions = document.querySelector("#color-options");
const form = document.querySelector("#chat-form");
const messageInput = document.querySelector("#message-input");
const nameInput = document.querySelector("#name-input");
const sendButton = document.querySelector("#send-button");
const connectionStatus = document.querySelector("#connection-status");
const connectionLabel = document.querySelector("#connection-label");
const participantCount = document.querySelector("#participant-count");
const lockScreen = document.querySelector("#lock-screen");

let selectedColor = COLORS[3].value;
let socket;
let reconnectDelay = 1000;
let enteredDigits = "";

function createColorOptions() {
  for (const color of COLORS) {
    const button = document.createElement("button");
    button.className = "color-option";
    button.type = "button";
    button.style.backgroundColor = color.swatch;
    button.setAttribute("role", "radio");
    button.setAttribute("aria-label", color.label);
    button.setAttribute("aria-checked", String(color.value === selectedColor));
    button.title = `${color.label}を選択`;
    button.addEventListener("click", () => {
      selectedColor = color.value;
      for (const option of colorOptions.children) {
        option.setAttribute("aria-checked", String(option === button));
      }
      messageInput.focus();
    });
    colorOptions.append(button);
  }
}

function appendMessage(message) {
  emptyState?.remove();

  const item = document.createElement("article");
  item.className = "message";

  const meta = document.createElement("div");
  meta.className = "message-meta";

  const name = document.createElement("span");
  name.className = "message-name";
  name.textContent = message.name;

  const time = document.createElement("time");
  time.dateTime = new Date(message.timestamp).toISOString();
  time.textContent = new Intl.DateTimeFormat("ja-JP", {
    hour: "2-digit",
    minute: "2-digit",
  }).format(message.timestamp);

  const bubble = document.createElement("div");
  bubble.className = "message-bubble";
  bubble.style.backgroundColor = message.color;
  bubble.textContent = message.text;

  meta.append(name, time);
  item.append(meta, bubble);
  messagesElement.append(item);
  messagesElement.scrollTop = messagesElement.scrollHeight;
}

function setConnected(connected) {
  connectionStatus.classList.toggle("is-connected", connected);
  connectionStatus.classList.toggle("is-disconnected", !connected);
  connectionLabel.textContent = connected ? "オンライン" : "再接続中...";
  sendButton.disabled = !connected;
}

function connect() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  socket = new WebSocket(`${protocol}//${window.location.host}/ws`);
  setConnected(false);

  socket.addEventListener("open", () => {
    reconnectDelay = 1000;
    setConnected(true);
  });

  socket.addEventListener("message", (event) => {
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch {
      return;
    }

    if (payload.type === "init") {
      participantCount.textContent = `参加者 ${payload.participants} 人`;
      setLockState(payload.locked);
      for (const message of payload.messages) {
        appendMessage(message);
      }
    } else if (payload.type === "participants") {
      participantCount.textContent = `参加者 ${payload.count} 人`;
    } else if (payload.type === "message") {
      appendMessage(payload);
    } else if (payload.type === "lock_state") {
      setLockState(payload.locked);
    }
  });

  socket.addEventListener("close", () => {
    setConnected(false);
    window.setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 10000);
  });

  socket.addEventListener("error", () => {
    socket.close();
  });
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const text = messageInput.value.trim();
  if (!text || socket?.readyState !== WebSocket.OPEN) {
    return;
  }

  socket.send(JSON.stringify({
    type: "message",
    name: nameInput.value.trim() || "ゲスト",
    text,
    color: selectedColor,
  }));
  messageInput.value = "";
  messageInput.focus();
});

document.addEventListener("keydown", (event) => {
  const target = event.target;
  if (
    event.isComposing
    || event.repeat
    || event.ctrlKey
    || event.altKey
    || event.metaKey
    || (target instanceof Element && target.closest("input, textarea, select, [contenteditable='true']"))
  ) {
    enteredDigits = "";
    return;
  }

  if (!/^\d$/.test(event.key)) {
    enteredDigits = "";
    return;
  }

  enteredDigits = `${enteredDigits}${event.key}`.slice(-4);
  if (enteredDigits === "6780") {
    enteredDigits = "";
    if (socket?.readyState !== WebSocket.OPEN) {
      return;
    }

    socket.send(JSON.stringify({ type: "admin_lock" }));
  }
});

function setLockState(locked) {
  if (typeof locked !== "boolean") {
    return;
  }
  lockScreen.hidden = !locked;
  if (locked) {
    document.activeElement?.blur();
    lockScreen.focus();
  }
}

createColorOptions();
connect();
