// TickBags WA Assistant - Background Service Worker

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "ASK_AI") {
    handleAIRequest(message.payload).then(sendResponse).catch(err => {
      sendResponse({ error: err.message });
    });
    return true; // Keep channel open for async
  }
});

function cleanBaseUrl(value) {
  return String(value || "https://dashboard.tickbags.com").trim().replace(/\/+$/, "");
}

async function handleAIRequest({ customerMessage, chatName, conversationHistory }) {
  const stored = await chrome.storage.local.get(["tickbotBackendUrl", "tickbotExtensionKey"]);
  const backendUrl = cleanBaseUrl(stored.tickbotBackendUrl);
  const extensionKey = String(stored.tickbotExtensionKey || "").trim();
  const headers = { "Content-Type": "application/json" };
  if (extensionKey) headers["X-TickBot-Key"] = extensionKey;

  const endpoint = `${backendUrl}/api/tickbot/extension/draft`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 20000);
  let response;

  try {
    response = await fetch(endpoint, {
      method: "POST",
      headers,
      signal: controller.signal,
      body: JSON.stringify({
        customer_message: customerMessage,
        chat_name: chatName,
        conversation_history: (conversationHistory || []).slice(-10)
      })
    });
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error(`TickBot backend timed out after 20s: ${backendUrl}`);
    }
    throw new Error(`Cannot reach TickBot backend: ${error.message}`);
  } finally {
    clearTimeout(timeoutId);
  }

  const rawText = await response.text();
  let data = {};
  try {
    data = rawText ? JSON.parse(rawText) : {};
  } catch {
    data = {};
  }
  if (!response.ok) {
    const detail = data.error || rawText.slice(0, 160) || response.statusText || "Request failed";
    throw new Error(`TickBot backend ${response.status}: ${detail}`);
  }
  if (!data.success || !data.reply) {
    throw new Error(data.error || "TickBot did not return a draft");
  }
  return { reply: String(data.reply).trim(), decision: data.decision || {} };
}
