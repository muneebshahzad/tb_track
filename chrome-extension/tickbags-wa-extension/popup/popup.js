// TickBags WA Assistant - Popup Script

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
    "globalAIEnabled", "disabledChats", "tickbotBackendUrl", "tickbotExtensionKey",
    "statsReplied", "statsChats"
  ]);

  const globalEnabled = stored.globalAIEnabled !== false;
  const disabledChats = new Set(stored.disabledChats || []);
  const backendUrl = stored.tickbotBackendUrl || "https://dashboard.tickbags.com";
  const extensionKey = stored.tickbotExtensionKey || "";

  // Global toggle
  const globalToggle = document.getElementById("global-toggle");
  globalToggle.checked = globalEnabled;
  globalToggle.addEventListener("change", async (e) => {
    await chrome.storage.local.set({ globalAIEnabled: e.target.checked });
    // Tell content script
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab?.url?.includes("web.whatsapp.com")) {
      chrome.tabs.sendMessage(tab.id, { type: "TOGGLE_GLOBAL", value: e.target.checked });
    }
    updateHeaderStatus(e.target.checked, null, disabledChats);
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
  let currentChatId = null;

  if (tab?.url?.includes("web.whatsapp.com")) {
    chrome.tabs.sendMessage(tab.id, { type: "GET_CURRENT_CHAT" }, (response) => {
      if (chrome.runtime.lastError || !response?.chatId) {
        document.getElementById("current-chat-name").textContent = "No chat open";
        document.getElementById("chat-toggle").disabled = true;
        return;
      }

      currentChatId = response.chatId;
      const chatToggle = document.getElementById("chat-toggle");
      chatToggle.disabled = false;
      chatToggle.checked = !disabledChats.has(currentChatId);
      document.getElementById("current-chat-name").textContent = currentChatId;

      chatToggle.addEventListener("change", async (e) => {
        chrome.tabs.sendMessage(tab.id, { type: "TOGGLE_CHAT", value: e.target.checked });
        if (!e.target.checked) {
          disabledChats.add(currentChatId);
        } else {
          disabledChats.delete(currentChatId);
        }
        await chrome.storage.local.set({ disabledChats: [...disabledChats] });
        updateHeaderStatus(globalToggle.checked, currentChatId, disabledChats);
      });

      updateHeaderStatus(globalEnabled, currentChatId, disabledChats);
    });
  } else {
    updateHeaderStatus(false, null, disabledChats);
    document.getElementById("header-status-text").textContent = "Open WhatsApp Web";
  }

  // Stats
  document.getElementById("stat-replied").textContent = stored.statsReplied || 0;
  document.getElementById("stat-chats").textContent = stored.statsChats || 0;
  document.getElementById("stat-disabled").textContent = disabledChats.size;

  if (!tab?.url?.includes("web.whatsapp.com")) {
    updateHeaderStatus(false, null, disabledChats);
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

function updateHeaderStatus(globalOn, chatId, disabledChats) {
  const dot = document.getElementById("header-status-dot");
  const text = document.getElementById("header-status-text");

  if (!globalOn) {
    dot.className = "status-dot off";
    text.textContent = "AI Paused (Global)";
    return;
  }
  if (!chatId) {
    dot.className = "status-dot on";
    text.textContent = "AI Active — open a chat";
    return;
  }
  if (disabledChats?.has(chatId)) {
    dot.className = "status-dot off";
    text.textContent = "Paused for this chat";
    return;
  }
  dot.className = "status-dot on";
  text.textContent = "AI Active ✓";
}

// Handle messages from content.js
chrome.runtime.onMessage.addListener((msg) => {
  if (msg.type === "STATE_CHANGED") {
    document.getElementById("global-toggle").checked = msg.globalAIEnabled;
  }
});

init();
