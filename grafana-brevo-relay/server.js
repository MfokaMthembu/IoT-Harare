
const express = require("express");

const app = express();
app.use(express.json({ limit: "256kb" }));

const {
  BREVO_API_KEY,
  ALERT_FROM_EMAIL,
  ALERT_FROM_NAME = "Grafana Alerts",
  ALERT_TO_EMAIL = "admin@example.com,network@example.com,alerts@example.com",
  RELAY_SECRET,
} = process.env;

const required = {
  BREVO_API_KEY,
  ALERT_FROM_EMAIL,
  ALERT_TO_EMAIL,
  RELAY_SECRET,
};

function checkConfig() {
  const missing = Object.entries(required)
    .filter(([, value]) => !value)
    .map(([key]) => key);

  if (missing.length) {
    throw new Error(`Missing environment variables: ${missing.join(", ")}`);
  }
}

function authorized(req) {
  const supplied = req.get("x-relay-secret") || "";
  const expected = RELAY_SECRET || "";

  return (
    supplied.length === expected.length &&
    supplied.length > 0 &&
    require("crypto").timingSafeEqual(
      Buffer.from(supplied),
      Buffer.from(expected)
    )
  );
}

async function sendEmail(subject, textContent) {
  checkConfig();

  const response = await fetch(
    "https://api.brevo.com/v3/smtp/email",
    {
      method: "POST",
      headers: {
        accept: "application/json",
        "api-key": BREVO_API_KEY,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        sender: {
          name: ALERT_FROM_NAME,
          email: ALERT_FROM_EMAIL,
        },
        to: ALERT_TO_EMAIL.split(",").map(email => ({ email: email.trim() })),
        subject,
        textContent,
      }),
      signal: AbortSignal.timeout(15000),
    }
  );

  if (!response.ok) {
    const details = await response.text();
    console.error("Brevo API error:", response.status, details);
    throw new Error(`Brevo returned HTTP ${response.status}`);
  }

  return response.json();
}

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

app.post("/test-email", async (req, res) => {
  if (!authorized(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    const result = await sendEmail(
      "Grafana relay test",
      "Success! Your Grafana-to-Brevo email relay can send email."
    );

    res.json({ status: "accepted", messageId: result.messageId });
  } catch (error) {
    console.error("Test email failed:", error.message);
    res.status(502).json({ error: "Email delivery request failed" });
  }
});

app.post("/grafana-webhook", async (req, res) => {
  if (!authorized(req)) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const payload = req.body || {};
  const status = payload.status || "unknown";
  const title = payload.title || "Grafana alert notification";
  const alerts = Array.isArray(payload.alerts) ? payload.alerts : [];

  const details = alerts.map((alert, index) => {
    const labels = JSON.stringify(alert.labels || {});
    const annotations = JSON.stringify(alert.annotations || {});

    return [
      `Alert ${index + 1}`,
      `Status: ${alert.status || status}`,
      `Labels: ${labels}`,
      `Details: ${annotations}`,
      `Starts at: ${alert.startsAt || "Unknown"}`,
    ].join("\n");
  });

  const message = [
    `Grafana notification status: ${status}`,
    `Title: ${title}`,
    payload.message || "",
    ...details,
  ].filter(Boolean).join("\n\n");

  try {
    const result = await sendEmail(
      `[Grafana ${status.toUpperCase()}] ${title}`.slice(0, 200),
      message.slice(0, 20000)
    );

    res.json({ status: "accepted", messageId: result.messageId });
  } catch (error) {
    console.error("Alert email failed:", error.message);
    res.status(502).json({ error: "Email request failed" });
  }
});

const port = process.env.PORT || 3000;

app.listen(port, "0.0.0.0", () => {
  console.log(`Grafana email relay listening on port ${port}`);
});
