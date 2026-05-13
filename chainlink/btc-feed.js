/**
 * BTC/USD price feed using Coinbase Advanced Trade WebSocket.
 * Outputs prices in the format: "CL: <price>"
 * which dash.py parses via its cl_on_message() handler.
 *
 * Coinbase is used as an oracle-style price source because:
 *  - It has slight timing differences vs Binance (real oracle lag)
 *  - It is publicly accessible without authentication
 *
 * Output format (one line per update):
 *   CL: 103421.5
 */

const WebSocket = require("ws");

const WS_URL = "wss://advanced-trade-ws.coinbase.com";
const PRODUCT_ID = "BTC-USD";
const RECONNECT_DELAY_MS = 3000;

let shouldStop = false;

process.on("SIGINT", () => { shouldStop = true; process.exit(0); });
process.on("SIGTERM", () => { shouldStop = true; process.exit(0); });

function connect() {
  if (shouldStop) return;

  const ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    // Subscribe to ticker channel
    const subMsg = JSON.stringify({
      type: "subscribe",
      product_ids: [PRODUCT_ID],
      channel: "ticker",
    });
    ws.send(subMsg);
  });

  ws.on("message", (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      // Handle ticker events
      if (data.channel === "ticker" && data.events) {
        for (const event of data.events) {
          if (event.type === "update" && event.tickers) {
            for (const ticker of event.tickers) {
              if (ticker.product_id === PRODUCT_ID && ticker.price) {
                const price = parseFloat(ticker.price);
                if (!isNaN(price) && price > 0) {
                  process.stdout.write(`CL: ${price}\n`);
                }
              }
            }
          }
        }
      }
    } catch (e) {
      // Silently ignore parse errors
    }
  });

  ws.on("error", () => {
    // Silently reconnect on error
  });

  ws.on("close", () => {
    if (!shouldStop) {
      setTimeout(connect, RECONNECT_DELAY_MS);
    }
  });
}

connect();
