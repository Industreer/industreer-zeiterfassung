// backend/sharepoint.js
// Lädt eine Excel-Datei über Microsoft Graph anhand eines SharePoint "Copy link" URLs.

function encodeSharingUrlToShareId(url) {
  // Graph /shares encoding:
  // 1) base64(url)
  // 2) base64url ohne '=' und mit -/_ statt +/  (un-padded)
  // 3) prefix "u!"
  // Docs: /shares-get
  const b64 = Buffer.from(url, "utf8").toString("base64");
  const b64url = b64.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
  return "u!" + b64url;
}

async function getGraphTokenClientCredentials() {
  const tenant = process.env.MS_TENANT_ID;
  const clientId = process.env.MS_CLIENT_ID;
  const clientSecret = process.env.MS_CLIENT_SECRET;

  if (!tenant || !clientId || !clientSecret) {
    throw new Error("MS_TENANT_ID/MS_CLIENT_ID/MS_CLIENT_SECRET fehlen (Entra App nötig)");
  }

  // Client credentials flow (v2 endpoint)
  const tokenUrl = `https://login.microsoftonline.com/${encodeURIComponent(tenant)}/oauth2/v2.0/token`;

  const body = new URLSearchParams();
  body.set("client_id", clientId);
  body.set("client_secret", clientSecret);
  body.set("grant_type", "client_credentials");
  body.set("scope", "https://graph.microsoft.com/.default");

  const r = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  const j = await r.json();
  if (!r.ok) {
    throw new Error(`Graph token error: ${j.error || r.status} ${j.error_description || ""}`.trim());
  }
  if (!j.access_token) throw new Error("Kein access_token von Graph erhalten");
  return j.access_token;
}

async function downloadExcelFromShareLink(sharedUrl) {
  if (!sharedUrl || !/^https:\/\/.+/.test(sharedUrl)) {
    throw new Error("Ungültige SharePoint URL");
  }

  const token = await getGraphTokenClientCredentials();

  const shareId = encodeSharingUrlToShareId(sharedUrl);

  // Direkt Download des Contents:
  // GET /shares/{shareId}/driveItem/content
  // Docs: driveItem-get-content
  const url = `https://graph.microsoft.com/v1.0/shares/${shareId}/driveItem/content`;

  const r = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      // optional: "Prefer": "redeemSharingLink"
    },
  });

  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`Graph download failed (${r.status}): ${txt || r.statusText}`);
  }

  const ab = await r.arrayBuffer();
  return Buffer.from(ab);
}

module.exports = {
  downloadExcelFromShareLink,
  encodeSharingUrlToShareId,
};
