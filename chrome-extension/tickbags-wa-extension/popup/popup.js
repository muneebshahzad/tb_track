// TickBags WA Assistant - Popup Script
let popupCurrentChatId = null;

async function getContentState() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.url?.includes("web.whatsapp.com")) return null;

  return new Promise((resolve) => {
    chrome.tabs.sendMessage(tab.id, { type: "GET_STATE" }, (response) => {
      if (chrome.runtime.lastError) resolve(null);
      else resolve(response);
    });
  });
}

async function init() {
  const stored = await chrome.storage.local.get([
    "globalAIEnabled", "disabledChats", "onlyChatId", "tickbotBackendUrl", "tickbotExtensionKey",
    "statsReplied", "statsChats"
  ]);

  let globalEnabled = stored.globalAIEnabled !== false;
  const disabledChats = new Set(stored.disabledChats || []);
  let onlyChatId = stored.onlyChatId || "";
  const backendUrl = stored.tickbotBackendUrl || "https://dashboard.tickbags.com";
  const extensionKey = stored.tickbotExtensionKey || "";

  // Global toggle
  const globalToggle = document.getElementById("global-toggle");
  globalToggle.checked = globalEnabled;
  globalToggle.addEventListener("change", async (e) => {
    globalEnabled = e.target.checked;
    if (globalEnabled) onlyChatId = "";
    await chrome.storage.local.set({ globalAIEnabled: globalEnabled, onlyChatId });
    // Tell content script
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab?.url?.includes("web.whatsapp.com")) {
      chrome.tabs.sendMessage(tab.id, { type: "TOGGLE_GLOBAL", value: globalEnabled });
    }
    document.getElementById("only-chat-toggle").checked = false;
    updateHeaderStatus(globalEnabled, popupCurrentChatId, disabledChats, onlyChatId);
  });

  // Backend settings
  const backendInput = document.getElementById("backend-url-input");
  const keyInput = document.getElementById("backend-key-input");
  backendInput.value = backendUrl;
  backendInput.classList.add("saved");
  if (extensionKey) {
    keyInput.value = extensionKey;
    keyInput.classList.add("saved");
  }

  document.getElementById("toggle-eye").addEventListener("click", () => {
    keyInput.type = keyInput.type === "password" ? "text" : "password";
  });

  document.getElementById("save-api-btn").addEventListener("click", async () => {
    const nextBackendUrl = cleanBackendUrl(backendInput.value);
    const nextExtensionKey = keyInput.value.trim();
    if (!isValidBackendUrl(nextBackendUrl)) {
      backendInput.style.borderColor = "#ff5252";
      setTimeout(() => backendInput.style.borderColor = "", 2000);
      return;
    }
    await chrome.storage.local.set({
      tickbotBackendUrl: nextBackendUrl,
      tickbotExtensionKey: nextExtensionKey
    });
    backendInput.value = nextBackendUrl;
    backendInput.classList.add("saved");
    keyInput.classList.toggle("saved", !!nextExtensionKey);
    const saveBtn = document.getElementById("save-api-btn");
    saveBtn.textContent = "✓ Saved!";
    saveBtn.classList.add("saved");
    document.getElementById("saved-msg").style.display = "block";
    setTimeout(() => {
      saveBtn.textContent = "Save Backend";
      saveBtn.classList.remove("saved");
    }, 2000);
    // Tell content script
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab?.url?.includes("web.whatsapp.com")) {
      chrome.tabs.sendMessage(tab.id, {
        type: "BACKEND_SETTINGS_UPDATED",
        backendUrl: nextBackendUrl
      });
    }
  });

  // Chat toggle — get current chat from content script
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (tab?.url?.includes("web.whatsapp.com")) {
    chrome.tabs.sendMessage(tab.id, { type: "GET_CURRENT_CHAT" }, (response) => {
      if (chrome.runtime.lastError || !response?.chatId) {
        document.getElementById("current-chat-name").textContent = "No chat open";
        document.getElementById("chat-toggle").disabled = true;
        return;
      }

      popupCurrentChatId = response.chatId;
      const chatToggle = document.getElementById("chat-toggle");
      const onlyChatToggle = document.getElementById("only-chat-toggle");
      chatToggle.disabled = false;
      onlyChatToggle.disabled = false;
      chatToggle.checked = !disabledChats.has(popupCurrentChatId);
      onlyChatToggle.checked = onlyChatId === popupCurrentChatId;
      document.getElementById("current-chat-name").textContent = popupCurrentChatId;

      chatToggle.addEventListener("change", async (e) => {
        chrome.tabs.sendMessage(tab.id, { type: "TOGGLE_CHAT", value: e.target.checked });
        if (!e.target.checked) {
          disabledChats.add(popupCurrentChatId);
        } else {
          disabledChats.delete(popupCurrentChatId);
        }
        await chrome.storage.local.set({ disabledChats: [...disabledChats] });
        updateHeaderStatus(globalEnabled, popupCurrentChatId, disabledChats, onlyChatId);
      });

      onlyChatToggle.addEventListener("change", async (e) => {
        onlyChatId = e.target.checked ? popupCurrentChatId : "";
        if (onlyChatId) {
          globalEnabled = false;
          globalToggle.checked = false;
          disabledChats.delete(popupCurrentChatId);
          chatToggle.checked = true;
        }
        await chrome.storage.local.set({
          onlyChatId,
          globalAIEnabled: globalEnabled,
          disabledChats: [...disabledChats]
        });
        chrome.tabs.sendMessage(tab.id, { type: "TOGGLE_ONLY_CHAT", value: e.target.checked });
        updateHeaderStatus(globalEnabled, popupCurrentChatId, disabledChats, onlyChatId);
      });

      updateHeaderStatus(globalEnabled, popupCurrentChatId, disabledChats, onlyChatId);
    });
  } else {
    updateHeaderStatus(false, null, disabledChats, onlyChatId);
    document.getElementById("header-status-text").textContent = "Open WhatsApp Web";
  }

  // Stats
  document.getElementById("stat-replied").textContent = stored.statsReplied || 0;
  document.getElementById("stat-chats").textContent = stored.statsChats || 0;
  document.getElementById("stat-disabled").textContent = disabledChats.size;

  if (!tab?.url?.includes("web.whatsapp.com")) {
    updateHeaderStatus(false, null, disabledChats, onlyChatId);
    document.getElementById("header-status-text").textContent = "Open WhatsApp Web";
  }
}

function cleanBackendUrl(value) {
  return String(value || "https://dashboard.tickbags.com").trim().replace(/\/+$/, "");
}

function isValidBackendUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.hostname === "localhost" || url.hostname === "127.0.0.1";
  } catch {
    return false;
  }
}

function updateHeaderStatus(globalOn, chatId, disabledChats, onlyChatId = "") {
  const dot = document.getElementById("header-status-dot");
  const text = document.getElementById("header-status-text");

  if (!chatId) {
    dot.className = globalOn ? "status-dot on" : "status-dot off";
    text.textContent = globalOn ? "AI Active — open a chat" : "Open a chat";
    return;
  }
  if (disabledChats?.has(chatId)) {
    dot.className = "status-dot off";
    text.textContent = "Paused for this chat";
    return;
  }
  if (onlyChatId === chatId) {
    dot.className = "status-dot on";
    text.textContent = "Only This Chat active";
    return;
  }
  if (onlyChatId) {
    dot.className = "status-dot off";
    text.textContent = "Only another chat active";
    return;
  }
  if (globalOn) {
    dot.className = "status-dot on";
    text.textContent = "AI Active globally";
    return;
  }
  dot.className = "status-dot off";
  text.textContent = "AI Paused";
}

// Handle messages from content.js
chrome.runtime.onMessage.addListener((msg) => {
  if (msg.type === "STATE_CHANGED") {
    document.getElementById("global-toggle").checked = msg.globalAIEnabled;
    const onlyToggle = document.getElementById("only-chat-toggle");
    if (onlyToggle) onlyToggle.checked = !!popupCurrentChatId && msg.onlyChatId === popupCurrentChatId;
  }
});

init();
