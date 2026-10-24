
const express = require("express");
const crypto = require("crypto");

const app = express();
app.use(express.json({ limit: "256kb" }));

const {
  BREVO_API_KEY,
  ALERT_FROM_EMAIL,
  ALERT_FROM_NAME = "",
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
    crypto.timingSafeEqual(
      Buffer.from(supplied),
      Buffer.from(expected)
    )
  );
}

// Escape dynamic values before inserting them into HTML.
function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[char]);
}

function formatDate(value) {
  if (!value) return "Not available";

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return date.toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC");
}

function formatStatus(value) {
  const status = String(value || "unknown").toUpperCase();

  if (status === "FIRING") return "FIRING";
  if (status === "RESOLVED") return "RESOLVED";

  return status;
}

function buildAlertEmail(payload) {
  const alerts = Array.isArray(payload.alerts) ? payload.alerts : [];
  const overallStatus = formatStatus(payload.status);
  const firingCount = alerts.filter(
    (alert) => String(alert.status || payload.status).toLowerCase() === "firing"
  ).length;
  const resolvedCount = alerts.filter(
    (alert) => String(alert.status || payload.status).toLowerCase() === "resolved"
  ).length;

  const statusWord =
    overallStatus === "FIRING"
      ? "Active"
      : overallStatus === "RESOLVED"
        ? "Resolved"
        : overallStatus;

  const subject = `[${overallStatus}] Water & Sewer Monitoring - ${
    alerts.length
  } alert${alerts.length === 1 ? "" : "s"}`;

  const alertSections = alerts.map((alert, index) => {
    const labels = alert.labels || {};
    const annotations = alert.annotations || {};

    const sensor = labels.sensor_id || "Sensor not specified";
    const metric = labels.metric_type || "Metric not specified";
    const alertStatus = formatStatus(alert.status || payload.status);
    const summary =
      annotations.summary ||
      annotations.description ||
      "A monitoring alert condition has been detected.";

    // Show actual readings only when the webhook provides them.
    const values = alert.values || {};
    const actualReading =
      annotations.current_value ??
      annotations.currentValue ??
      values.current_value ??
      values.currentValue;

    const unit = annotations.unit || "";
    const normalMin = annotations.normal_min ?? annotations.normalMin;
    const normalMax = annotations.normal_max ?? annotations.normalMax;
    const critMin = annotations.crit_min ?? annotations.critMin;
    const critMax = annotations.crit_max ?? annotations.critMax;

    const details = [];

    if (actualReading !== undefined) {
      details.push(
        `<tr>
          <td style="padding:8px 0;color:#64748b;">Current reading</td>
          <td style="padding:8px 0;text-align:right;font-weight:600;">${escapeHtml(actualReading)} ${escapeHtml(unit)}</td>
        </tr>`
      );
    }

    if (normalMin !== undefined || normalMax !== undefined) {
      details.push(
        `<tr>
          <td style="padding:8px 0;color:#64748b;">Normal range</td>
          <td style="padding:8px 0;text-align:right;">${escapeHtml(normalMin ?? "—")} – ${escapeHtml(normalMax ?? "—")} ${escapeHtml(unit)}</td>
        </tr>`
      );
    }

    if (critMin !== undefined || critMax !== undefined) {
      details.push(
        `<tr>
          <td style="padding:8px 0;color:#64748b;">Critical limits</td>
          <td style="padding:8px 0;text-align:right;">${escapeHtml(critMin ?? "—")} – ${escapeHtml(critMax ?? "—")} ${escapeHtml(unit)}</td>
        </tr>`
      );
    }

    const statusColor =
      alertStatus === "FIRING" ? "#b91c1c" : "#047857";

    return {
      text: [
        `${index + 1}. ${sensor}`,
        `Metric: ${metric}`,
        `Status: ${alertStatus}`,
        `Details: ${summary}`,
        actualReading !== undefined
          ? `Current reading: ${actualReading} ${unit}`.trim()
          : "",
        `Started: ${formatDate(alert.startsAt)}`,
      ].filter(Boolean).join("\n"),

      html: `
        <div style="border:1px solid #e2e8f0;border-radius:10px;margin:0 0 16px;padding:18px;">
          <div style="font-size:12px;color:#64748b;margin-bottom:6px;">
            SENSOR ${index + 1}
          </div>
          <div style="font-size:18px;font-weight:700;color:#0f172a;margin-bottom:8px;">
            ${escapeHtml(sensor)}
          </div>

          <div style="margin-bottom:12px;">
            <span style="background:${statusColor};color:#ffffff;border-radius:4px;padding:4px 8px;font-size:11px;font-weight:700;">
              ${escapeHtml(alertStatus)}
            </span>
            <span style="margin-left:8px;color:#475569;font-size:13px;">
              ${escapeHtml(metric.replace(/_/g, " "))}
            </span>
          </div>

          <p style="font-size:14px;line-height:1.6;color:#334155;margin:0 0 10px;">
            ${escapeHtml(summary)}
          </p>

          ${
            details.length
              ? `<table role="presentation" style="width:100%;border-collapse:collapse;font-size:13px;">
                  ${details.join("")}
                </table>`
              : ""
          }

          <div style="font-size:12px;color:#64748b;margin-top:12px;">
            Started: ${escapeHtml(formatDate(alert.startsAt))}
          </div>
        </div>
      `,
    };
  });

  const summaryLine = alerts.length
    ? `${firingCount} active alert${firingCount === 1 ? "" : "s"}${
        resolvedCount ? ` · ${resolvedCount} resolved` : ""
      }`
    : "Grafana sent a notification without individual alert details.";

  const htmlContent = `
    <!doctype html>
    <html>
      <body style="margin:0;padding:24px;background:#f1f5f9;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
        <div style="max-width:680px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
          <div style="background:#0f172a;padding:26px 24px;color:#ffffff;">
            <div style="font-size:12px;letter-spacing:1px;text-transform:uppercase;color:#cbd5e1;">
              HRE Water &amp; Sewer Monitoring System
            </div>
            <h1 style="font-size:23px;margin:10px 0 8px;">
              ${overallStatus === "FIRING" ? "Telemetry Alert" : "Alert Update"}
            </h1>
            <div style="font-size:14px;color:#e2e8f0;">
              ${escapeHtml(summaryLine)}
            </div>
          </div>

          <div style="padding:24px;">
            <p style="font-size:14px;line-height:1.6;color:#334155;margin-top:0;">
              ${
                overallStatus === "FIRING"
                  ? "The monitoring system has detected readings that meet an alert condition. Review the affected sensors and investigate as appropriate."
                  : "The monitoring system has sent an update about the alert conditions listed below."
              }
            </p>

            ${
              alertSections.length
                ? alertSections.map((section) => section.html).join("")
                : `<p style="color:#475569;">No individual alert instances were included in this notification.</p>`
            }

            <div style="border-top:1px solid #e2e8f0;padding-top:16px;margin-top:20px;font-size:12px;line-height:1.6;color:#64748b;">
              Automated notification from Grafana Alerting.<br>
              Please kindly check your monitoring dashboard to investigate the readings.
            </div>
          </div>
        </div>
      </body>
    </html>
  `;

  const textContent = [
    "HRE WATER & SEWER MONITORING SYSTEM",
    "================================",
    `Status: ${overallStatus}`,
    summaryLine,
    "",
    ...alertSections.map((section) => section.text),
    "",
    "Automated notification from Grafana Alerting.",
    "Please kindly check your monitoring dashboard to investigate the readings.",
  ].join("\n");

  return { subject, textContent, htmlContent };
}

async function sendEmail(subject, textContent, htmlContent) {
  checkConfig();

  const recipients = ALERT_TO_EMAIL
    .split(",")
    .map((email) => email.trim())
    .filter(Boolean)
    .map((email) => ({ email }));

  if (!recipients.length) {
    throw new Error("No email recipients configured");
  }

  const body = {
    sender: {
      name: ALERT_FROM_NAME,
      email: ALERT_FROM_EMAIL,
    },
    to: recipients,
    subject,
    textContent,
  };

  if (htmlContent) {
    body.htmlContent = htmlContent;
  }

  const response = await fetch(
    "https://api.brevo.com/v3/smtp/email",
    {
      method: "POST",
      headers: {
        accept: "application/json",
        "api-key": BREVO_API_KEY,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
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
      "HRE Water & Sewer Monitoring - Email Test",
      "Success! Your monitoring system can send email through the relay.",
      `<div style="font-family:Arial,sans-serif;max-width:600px;margin:auto;padding:24px;">
        <h2 style="color:#0f172a;">HRE Water &amp; Sewer Monitoring</h2>
        <p>Greater HRE Water & Sewer Monitoring System.</p>
      </div>`
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

  try {
    const { subject, textContent, htmlContent } = buildAlertEmail(
      req.body || {}
    );

    const result = await sendEmail(subject, textContent, htmlContent);

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
