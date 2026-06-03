// TickBags WA Assistant - Content Script v1.1 (robust selectors)

let globalAIEnabled = true;
let disabledChats = new Set();
let onlyChatId = "";
let backendUrl = "https://dashboard.tickbags.com";
let currentChatId = null;
let chatConversationHistory = {};
let lastProcessedMsgKey = null;
let processingKey = null;
const processedMessageKeys = new Set();
let floatingPanel = null;

// ─── INIT ───────────────────────────────────────────────────────────────────

async function init() {
  const stored = await chrome.storage.local.get(["globalAIEnabled", "disabledChats", "onlyChatId", "tickbotBackendUrl"]);
  globalAIEnabled = stored.globalAIEnabled !== false;
  disabledChats = new Set(stored.disabledChats || []);
  onlyChatId = stored.onlyChatId || "";
  backendUrl = stored.tickbotBackendUrl || "https://dashboard.tickbags.com";

  injectFloatingPanel();
  updatePanelState();
  startMessagePolling();

  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (msg.type === "TOGGLE_GLOBAL") {
      globalAIEnabled = msg.value;
      if (globalAIEnabled) onlyChatId = "";
      updatePanelState();
    }
    if (msg.type === "TOGGLE_CHAT") {
      if (msg.value) disabledChats.delete(currentChatId);
      else disabledChats.add(currentChatId);
      updatePanelState();
    }
    if (msg.type === "TOGGLE_ONLY_CHAT") {
      onlyChatId = msg.value && currentChatId ? currentChatId : "";
      if (onlyChatId) globalAIEnabled = false;
      updatePanelState();
    }
    if (msg.type === "BACKEND_SETTINGS_UPDATED") { backendUrl = msg.backendUrl || backendUrl; }
    if (msg.type === "GET_CURRENT_CHAT") { sendResponse({ chatId: currentChatId || getCurrentChatId() }); return true; }
    if (msg.type === "GET_STATE") {
      sendResponse({
        globalAIEnabled,
        currentChatId,
        chatEnabled: currentChatId ? !disabledChats.has(currentChatId) : true,
        onlyChatId
      });
      return true;
    }
  });
}

// ─── CHAT DETECTION ─────────────────────────────────────────────────────────

function getCurrentChatId() {
  // Try multiple selectors — WhatsApp changes these
  const selectors = [
    '[data-testid="conversation-header"] span[title]',
    'header span[title]',
    '[data-testid="contact-info-subtitle"]',
    '#main header span[dir="auto"]'
  ];
  for (const sel of selectors) {
    const el = document.querySelector(sel);
    if (el) {
      const title = el.getAttribute("title") || el.textContent;
      if (title && title.length > 0) return title.trim();
    }
  }
  return null;
}

// ─── POLLING-BASED MESSAGE DETECTION ─────────────────────────────────────────
// More reliable than MutationObserver with WhatsApp's virtual DOM

function startMessagePolling() {
  setInterval(() => {
    try {
      checkForNewMessages();
    } catch(e) {
      // Silent fail — keep polling
    }
  }, 1500);
}

function checkForNewMessages() {
  const chatId = getCurrentChatId();
  if (chatId) currentChatId = chatId;

  updatePanelState();
  if (!currentChatId) return;
  if (!isCurrentChatActive() || !backendUrl) {
    markVisibleIncomingMessagesHandled();
    return;
  }

  const messages = getVisibleIncomingMessages();
  if (!messages.length) {
    return checkViaFocusableItems();
  }

  const latestMessage = messages[messages.length - 1];
  if (hasMessageBeenHandled(latestMessage.key)) return;
  markMessageProcessing(latestMessage.key);
  triggerAIReply(latestMessage.text, latestMessage.key);
}

function getVisibleIncomingMessages() {
  return getVisibleConversationMessages().filter(message => message.role === "user");
}

function getVisibleConversationMessages(limit = 16) {
  const candidates = [
    ...document.querySelectorAll('#main [role="row"]'),
    ...document.querySelectorAll('#main [data-testid="msg-container"]'),
    ...document.querySelectorAll('#main .message-in, #main [class*="message-in"]'),
    ...document.querySelectorAll('#main .message-out, #main [class*="message-out"]')
  ];
  const seen = new Set();
  const messages = [];

  for (const node of candidates) {
    const row = normalizeMessageRow(node);
    if (!row || seen.has(row)) continue;
    seen.add(row);
    const text = extractMessageText(row);
    if (!text) continue;
    messages.push({
      key: buildMessageKey(row, text),
      role: isOutgoingMessageRow(row) ? "assistant" : "user",
      text
    });
  }
  return messages.slice(-limit);
}

function normalizeMessageRow(node) {
  if (!node) return null;
  return node.closest('[role="row"]') || node.closest('[data-testid="msg-container"]') || node;
}

function isIncomingMessageRow(row) {
  return !isOutgoingMessageRow(row);
}

function isOutgoingMessageRow(row) {
  const className = String(row.className || "");
  if (className.includes("message-out") || row.querySelector('[class*="message-out"]')) return true;
  if (className.includes("message-in") || row.querySelector('[class*="message-in"]')) return false;
  const hasOutgoingTick = row.querySelector(
    '[data-testid="msg-dblcheck"], [data-testid="msg-check"], ' +
    '[data-icon="msg-dblcheck"], [data-icon="msg-check"], ' +
    '[aria-label="Read"], [aria-label="Delivered"], [aria-label="Sent"]'
  );
  return !!hasOutgoingTick;
}

function buildMessageKey(row, text) {
  const meta = row.querySelector('[data-pre-plain-text]')?.getAttribute("data-pre-plain-text") ||
               row.querySelector('[data-testid="msg-meta"]')?.textContent ||
               "";
  const rowId = row.getAttribute("data-id") || row.id || "";
  return `${currentChatId}|${rowId}|${meta}|${text}`.slice(0, 700);
}

function hasMessageBeenHandled(key) {
  return !key || processedMessageKeys.has(key) || key === lastProcessedMsgKey || key === processingKey;
}

function markVisibleIncomingMessagesHandled() {
  for (const message of getVisibleIncomingMessages()) {
    if (message.key) processedMessageKeys.add(message.key);
  }
  while (processedMessageKeys.size > 80) {
    processedMessageKeys.delete(processedMessageKeys.values().next().value);
  }
}

function markMessageProcessing(key) {
  processingKey = key;
  lastProcessedMsgKey = key;
  processedMessageKeys.add(key);
  while (processedMessageKeys.size > 80) {
    processedMessageKeys.delete(processedMessageKeys.values().next().value);
  }
}

function checkViaFocusableItems() {
  // Alternative: scan all text-containing divs for incoming pattern
  const allCopyable = document.querySelectorAll('.copyable-text, [data-pre-plain-text]');
  if (!allCopyable.length) return;

  const last = allCopyable[allCopyable.length - 1];
  const container = last.closest('[role="row"]') || last.parentElement;
  
  if (!container) return;

  // Check it's not outgoing
  const hasCheck = container.querySelector('[data-testid*="check"], [data-icon*="check"]');
  const isOut = container.closest('[class*="message-out"]');
  if (hasCheck || isOut) return;

  const text = last.querySelector('span.selectable-text')?.innerText || last.innerText;
  if (!text?.trim()) return;

  const msgKey = buildMessageKey(container, text.trim());
  if (hasMessageBeenHandled(msgKey)) return;

  markMessageProcessing(msgKey);
  triggerAIReply(text.trim(), msgKey);
}

function extractMessageText(container) {
  // Try multiple ways to get text from a message bubble
  const attempts = [
    () => container.querySelector('span.selectable-text')?.innerText,
    () => container.querySelector('.copyable-text span')?.innerText,
    () => container.querySelector('[data-pre-plain-text]')?.querySelector('span')?.innerText,
    () => container.querySelector('.copyable-text')?.innerText,
    () => {
      // Last resort: get all text, strip timestamps
      const clone = container.cloneNode(true);
      clone.querySelectorAll('span[data-testid="msg-meta"], [class*="time"]').forEach(el => el.remove());
      return clone.innerText?.trim();
    }
  ];
  
  for (const fn of attempts) {
    try {
      const text = fn();
      if (text && text.trim().length > 0 && text.trim().length < 2000) {
        return text.trim();
      }
    } catch(e) {}
  }
  return null;
}

// ─── AI REPLY ────────────────────────────────────────────────────────────────

async function triggerAIReply(customerMessage, msgKey) {
  showTypingIndicator();

  if (!chatConversationHistory[currentChatId]) {
    chatConversationHistory[currentChatId] = [];
  }
  const visibleHistory = getVisibleConversationMessages(16);
  const history = visibleHistory.length ? visibleHistory.map(message => ({
    role: message.role,
    content: message.text
  })) : chatConversationHistory[currentChatId];

  try {
    const result = await chrome.runtime.sendMessage({
      type: "ASK_AI",
      payload: { customerMessage, chatName: currentChatId, conversationHistory: history }
    });

    hideTypingIndicator();

    if (result?.error) {
      showNotification("AI Error: " + result.error, "error");
      processingKey = null;
      return;
    }

    const reply = result?.reply;
    if (!reply) { processingKey = null; return; }

    history.push({ role: "user", content: customerMessage });
    history.push({ role: "assistant", content: reply });
    if (history.length > 10) history.splice(0, 2);

    const sent = sendWhatsAppMessage(reply);
    if (sent) {
      await recordSuggestionSent(currentChatId);
      showNotification("TickBags AI reply sent.", "success");
    }
    processingKey = null;

  } catch (err) {
    hideTypingIndicator();
    showNotification("Extension error: " + err.message, "error");
    processingKey = null;
  }
}

// ─── REPLY SUGGESTION PANEL ──────────────────────────────────────────────────

function showReplySuggestion(suggestedReply, originalMessage) {
  const existing = document.getElementById("tb-suggestion");
  if (existing) existing.remove();

  const panel = document.createElement("div");
  panel.id = "tb-suggestion";
  panel.innerHTML = `
    <div class="tb-suggestion-header">
      <span class="tb-icon">🤖</span>
      <span>AI Suggested Reply</span>
      <button class="tb-close-btn" id="tb-close-suggestion">✕</button>
    </div>
    <div class="tb-original-msg">
      <span class="tb-label">Customer:</span>
      <p>${escapeHtml(originalMessage)}</p>
    </div>
    <div class="tb-reply-text" id="tb-reply-text" contenteditable="true">${escapeHtml(suggestedReply)}</div>
    <div class="tb-suggestion-actions">
      <button class="tb-btn tb-btn-send" id="tb-send-reply">Send ✓</button>
      <button class="tb-btn tb-btn-edit" id="tb-regen-reply">↻ Regen</button>
      <button class="tb-btn tb-btn-dismiss" id="tb-dismiss-reply">Dismiss</button>
    </div>
  `;

  const footer = document.querySelector('[data-testid="conversation-footer"]') ||
                 document.querySelector('footer') ||
                 document.querySelector('[data-testid="compose-box-input"]')?.closest('div[style]');

  if (footer) {
    footer.parentNode.insertBefore(panel, footer);
  } else {
    document.querySelector("#main")?.appendChild(panel);
  }

  document.getElementById("tb-send-reply").onclick = () => {
    const text = document.getElementById("tb-reply-text").innerText.trim();
    sendWhatsAppMessage(text);
    recordSuggestionSent(currentChatId);
    panel.remove();
    processingKey = null;
  };

  document.getElementById("tb-dismiss-reply").onclick = () => { panel.remove(); processingKey = null; };
  document.getElementById("tb-close-suggestion").onclick = () => { panel.remove(); processingKey = null; };

  document.getElementById("tb-regen-reply").onclick = async () => {
    document.getElementById("tb-reply-text").innerText = "Regenerating...";
    const result = await chrome.runtime.sendMessage({
      type: "ASK_AI",
      payload: { customerMessage: originalMessage, chatName: currentChatId, conversationHistory: chatConversationHistory[currentChatId] || [] }
    });
    if (!result?.error) {
      document.getElementById("tb-reply-text").innerText = result.reply;
    }
  };
}

// ─── SEND MESSAGE ────────────────────────────────────────────────────────────

function sendWhatsAppMessage(text) {
  const inputSelectors = [
    '[data-testid="conversation-compose-box-input"]',
    '[contenteditable="true"][data-tab="10"]',
    '[contenteditable="true"][spellcheck="true"]',
    'footer [contenteditable="true"]',
    '#main [contenteditable="true"]'
  ];

  let inputBox = null;
  for (const sel of inputSelectors) {
    inputBox = document.querySelector(sel);
    if (inputBox) break;
  }

  if (!inputBox) {
    showNotification("⚠️ Could not find message input", "error");
    return false;
  }

  inputBox.focus();
  document.execCommand("insertText", false, text);
  inputBox.dispatchEvent(new Event("input", { bubbles: true }));

  setTimeout(() => {
    const sendBtn = document.querySelector('[data-testid="send"], [aria-label="Send"], [data-icon="send"]');
    if (sendBtn) {
      sendBtn.click();
    } else {
      inputBox.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", code: "Enter", keyCode: 13, bubbles: true }));
    }
  }, 300);
  return true;
}

async function recordSuggestionSent(chatId) {
  const stored = await chrome.storage.local.get(["statsReplied", "statsChatIds"]);
  const chatIds = new Set(stored.statsChatIds || []);
  if (chatId) chatIds.add(chatId);
  await chrome.storage.local.set({
    statsReplied: Number(stored.statsReplied || 0) + 1,
    statsChats: chatIds.size,
    statsChatIds: [...chatIds]
  });
}

// ─── FLOATING PANEL ──────────────────────────────────────────────────────────

function injectFloatingPanel() {
  floatingPanel = document.createElement("div");
  floatingPanel.id = "tb-floating-panel";
  floatingPanel.innerHTML = `
    <div class="tb-panel-logo">
      <span>🛋️</span>
      <span class="tb-brand">TickBags AI</span>
    </div>
    <div class="tb-toggle-row">
      <span class="tb-toggle-label">Global AI</span>
      <label class="tb-switch">
        <input type="checkbox" id="tb-global-toggle" ${globalAIEnabled ? "checked" : ""}>
        <span class="tb-slider"></span>
      </label>
    </div>
    <div class="tb-toggle-row" id="tb-chat-toggle-row">
      <span class="tb-toggle-label">This Chat</span>
      <label class="tb-switch">
        <input type="checkbox" id="tb-chat-toggle" checked>
        <span class="tb-slider"></span>
      </label>
    </div>
    <div class="tb-toggle-row" id="tb-only-chat-row">
      <span class="tb-toggle-label">Only This Chat</span>
      <label class="tb-switch">
        <input type="checkbox" id="tb-only-chat-toggle">
        <span class="tb-slider"></span>
      </label>
    </div>
    <div class="tb-status" id="tb-status-dot">
      <span class="tb-dot active"></span>
      <span id="tb-status-text">AI Active</span>
    </div>
  `;
  document.body.appendChild(floatingPanel);
  makeDraggable(floatingPanel);

  document.getElementById("tb-global-toggle").addEventListener("change", (e) => {
    globalAIEnabled = e.target.checked;
    if (globalAIEnabled) onlyChatId = "";
    chrome.storage.local.set({ globalAIEnabled, onlyChatId });
    updatePanelState();
    chrome.runtime.sendMessage({ type: "STATE_CHANGED", globalAIEnabled, onlyChatId });
  });

  document.getElementById("tb-chat-toggle").addEventListener("change", (e) => {
    if (!currentChatId) return;
    if (e.target.checked) disabledChats.delete(currentChatId);
    else disabledChats.add(currentChatId);
    chrome.storage.local.set({ disabledChats: [...disabledChats] });
    updatePanelState();
  });

  document.getElementById("tb-only-chat-toggle").addEventListener("change", (e) => {
    if (!currentChatId) {
      e.target.checked = false;
      return;
    }
    onlyChatId = e.target.checked ? currentChatId : "";
    if (onlyChatId) {
      globalAIEnabled = false;
      disabledChats.delete(currentChatId);
    }
    chrome.storage.local.set({ onlyChatId, globalAIEnabled, disabledChats: [...disabledChats] });
    updatePanelState();
    chrome.runtime.sendMessage({ type: "STATE_CHANGED", globalAIEnabled, onlyChatId });
  });
}

function isCurrentChatActive() {
  if (!currentChatId || disabledChats.has(currentChatId)) return false;
  return globalAIEnabled || onlyChatId === currentChatId;
}

function updatePanelState() {
  if (!floatingPanel) return;
  const globalToggle = document.getElementById("tb-global-toggle");
  const chatToggle = document.getElementById("tb-chat-toggle");
  const onlyChatToggle = document.getElementById("tb-only-chat-toggle");
  const statusDot = document.querySelector(".tb-dot");
  const statusText = document.getElementById("tb-status-text");
  const chatRow = document.getElementById("tb-chat-toggle-row");

  if (globalToggle) globalToggle.checked = globalAIEnabled;
  if (chatToggle && currentChatId) chatToggle.checked = !disabledChats.has(currentChatId);
  if (onlyChatToggle) onlyChatToggle.checked = !!currentChatId && onlyChatId === currentChatId;

  const isActive = isCurrentChatActive();
  if (statusDot) statusDot.className = "tb-dot " + (isActive ? "active" : "inactive");
  if (statusText) {
    if (!currentChatId) statusText.textContent = "No chat open";
    else if (disabledChats.has(currentChatId)) statusText.textContent = "AI Off (This Chat)";
    else if (onlyChatId === currentChatId) statusText.textContent = "AI Active (Only This Chat)";
    else if (onlyChatId) statusText.textContent = "AI Off (Only another chat)";
    else if (!globalAIEnabled) statusText.textContent = "AI Off (Global)";
    else statusText.textContent = "AI Active (Global)";
  }
  if (chatRow) chatRow.style.opacity = (globalAIEnabled || onlyChatId === currentChatId) ? "1" : "0.55";
}

function makeDraggable(el) {
  let isDragging = false, startX, startY, startLeft, startTop;
  el.addEventListener("mousedown", (e) => {
    if (e.target.tagName === "INPUT" || e.target.tagName === "LABEL" || e.target.closest("label")) return;
    isDragging = true;
    startX = e.clientX; startY = e.clientY;
    const rect = el.getBoundingClientRect();
    startLeft = rect.left; startTop = rect.top;
    e.preventDefault();
  });
  document.addEventListener("mousemove", (e) => {
    if (!isDragging) return;
    el.style.left = (startLeft + e.clientX - startX) + "px";
    el.style.top = (startTop + e.clientY - startY) + "px";
    el.style.right = "auto"; el.style.bottom = "auto";
  });
  document.addEventListener("mouseup", () => { isDragging = false; });
}

// ─── TYPING / NOTIFICATIONS ──────────────────────────────────────────────────

function showTypingIndicator() {
  if (document.getElementById("tb-typing")) return;
  const el = document.createElement("div");
  el.id = "tb-typing";
  el.innerHTML = `<span class="tb-dot-pulse"></span><span class="tb-dot-pulse"></span><span class="tb-dot-pulse"></span><span style="margin-left:8px;font-size:12px;color:#667781;">AI is thinking...</span>`;
  const footer = document.querySelector('[data-testid="conversation-footer"]') || document.querySelector('footer');
  if (footer) footer.parentNode.insertBefore(el, footer);
  else document.querySelector("#main")?.appendChild(el);
}

function hideTypingIndicator() { document.getElementById("tb-typing")?.remove(); }

function showNotification(message, type = "info") {
  const notif = document.createElement("div");
  notif.className = `tb-notification tb-notif-${type}`;
  notif.textContent = message;
  document.body.appendChild(notif);
  setTimeout(() => notif.remove(), 4000);
}

function escapeHtml(text) {
  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ─── START ───────────────────────────────────────────────────────────────────

const waitForWA = setInterval(() => {
  if (document.querySelector('#main') || document.querySelector('[data-testid="intro-md-beta-logo-dark"]')) {
    clearInterval(waitForWA);
    setTimeout(init, 2000);
  }
}, 500);
