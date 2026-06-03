# TickBags WA Assistant — Chrome Extension

AI-powered WhatsApp Business assistant for TickBags.com

---

## Installation

1. Open Chrome and go to `chrome://extensions`
2. Enable **Developer Mode** (top right toggle)
3. Click **"Load unpacked"**
4. Select this folder (`tickbags-wa-extension`)
5. The extension icon 🛋️ will appear in your toolbar

---

## Setup

1. Click the 🛋️ icon in Chrome toolbar
2. Confirm the **TickBot backend URL**. Default: `https://dashboard.tickbags.com`
3. Add the optional extension key if your backend has `TICKBOT_EXTENSION_KEY` or `INTERNAL_API_KEY` set
4. Click **Save Backend**
5. Open [web.whatsapp.com](https://web.whatsapp.com)
6. The floating AI panel will appear in the bottom-right

---

## How It Works

- When a customer sends a message, the extension asks the TickBot backend for a reply
- If Global AI is on and the current chat is enabled, the reply is inserted into WhatsApp Web and sent automatically
- Use the Global or per-chat toggle to pause automatic replies

---

## Controls

### Global Toggle
- In the popup or on the floating panel
- Turn off to pause AI for ALL chats

### Per-Chat Toggle  
- On the floating panel while a chat is open
- Disable AI for specific chats (e.g., chats with suppliers, personal contacts)
- Setting is remembered per chat name

---

## Language Support

The AI automatically detects:
- **English** → replies in English
- **Roman Urdu** → replies in Roman Urdu  
  Example: *"Delivery kitne din mein hogi?"* → *"Aap ka order 3-5 working days mein deliver ho jata hai InshaAllah!"*

---

## Order Tracking

The extension now asks the TickBot backend for drafts at:

```text
/api/tickbot/extension/draft
```

Order tracking should be added in the backend TickBot flow, not inside the extension. This keeps product facts, order lookup, AI prompts, and safety checks in one place.

---

## Files

```
tickbags-wa-extension/
├── manifest.json       — Extension config
├── background.js       — TickBot backend draft calls
├── content.js          — WhatsApp Web injection
├── content.css         — UI styles
├── popup/
│   ├── popup.html      — Extension popup UI
│   └── popup.js        — Popup logic
└── icons/              — Extension icons
```

---

## Troubleshooting

- **No suggestion appearing?** Make sure the backend URL is saved and Global AI is ON
- **Backend says Unauthorized?** Add the same key in the popup that is set as `TICKBOT_EXTENSION_KEY` or `INTERNAL_API_KEY`
- **Wrong language or wrong business info?** Update the TickBot backend prompt/settings, not this extension
