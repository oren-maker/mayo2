FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY . .

# Persistent data dirs (mounted as Railway volume)
RUN mkdir -p /data/saved-groups /data/prompt-details /data/brand-logs /data/exports /data/avatars

ENV DATA_DIR=/data
ENV PORT=3600

EXPOSE 3600

CMD ["node", "server.mjs"]
