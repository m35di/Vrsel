import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

// ??????? ?????? ?? ??????? ????
export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

// ????? ??? ????? ??? ???? ??? ??? ????
const _CORE_ADDR = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

// ???? ???? ????? ?? ???? ??? ?????
const IGNORE_FIELDS = new Set([
  "host", "connection", "keep-alive", "proxy-authenticate",
  "proxy-authorization", "te", "trailer", "transfer-encoding",
  "upgrade", "forwarded", "x-forwarded-host", "x-forwarded-proto",
  "x-forwarded-port",
]);

export default async function syncProvider(req, res) {
  // ???? ???? ?????????
  if (!_CORE_ADDR) {
    res.statusCode = 500;
    return res.end("System Error: Reference 001");
  }

  try {
    const _endpoint = _CORE_ADDR + req.url;

    const _mappedHeaders = {};
    let _originTag = null;

    // ??????? ? ??????? ????? ?? ???? ??? ??????
    for (const key of Object.keys(req.headers)) {
      const k = key.toLowerCase();
      const v = req.headers[key];
      
      if (IGNORE_FIELDS.has(k) || k.startsWith("x-vercel-")) continue;
      
      if (k === "x-real-ip" || k === "x-forwarded-for") {
        _originTag = Array.isArray(v) ? v[0] : v;
        continue;
      }
      _mappedHeaders[k] = Array.isArray(v) ? v.join(", ") : v;
    }
    
    if (_originTag) _mappedHeaders["x-forwarded-for"] = _originTag;

    const _m = req.method;
    const _payloadRequired = !["GET", "HEAD"].includes(_m);

    const _options = { 
      method: _m, 
      headers: _mappedHeaders, 
      redirect: "manual" 
    };

    if (_payloadRequired) {
      _options.body = Readable.toWeb(req);
      _options.duplex = "half";
    }

    // ????? ?????? ???? ??? ??? Fetch
    const _dataStream = await fetch(_endpoint, _options);

    res.statusCode = _dataStream.status;
    
    // ??? ???? ?????? ???????
    for (const [k, v] of _dataStream.headers) {
      if (k.toLowerCase() === "transfer-encoding") continue;
      try { res.setHeader(k, v); } catch (e) {}
    }

    if (_dataStream.body) {
      await pipeline(Readable.fromWeb(_dataStream.body), res);
    } else {
      res.end();
    }
  } catch (_err) {
    // ??????? ??? ????
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Data Sync Unavailable");
    }
  }
}
