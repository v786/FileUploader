/// <reference types="vitest" />
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The API is proxied so the app talks to it same-origin. Presigned URLs point straight at the
// object store (the mock S3 on :8000), like S3 in production: bytes never pass through the API.
const API = process.env.API_URL ?? "http://127.0.0.1:8000";
const proxied = ["/batches", "/files", "/admin", "/__faults", "/health"];

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: Object.fromEntries(proxied.map((p) => [p, { target: API, changeOrigin: true }])),
  },
  test: { environment: "node" },
});
