# Freebuff Endpoint — production container image.
FROM node:22-alpine

WORKDIR /app

# Deps first for layer caching
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src

ENV NODE_ENV=production
EXPOSE 8090

# /healthz is the unauthenticated liveness probe — /health sits behind
# API_KEYS and would fail the container once keys are configured.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8090/healthz >/dev/null || exit 1

CMD ["node", "src/server.js"]
