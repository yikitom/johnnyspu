FROM node:20-slim

# Install Chrome dependencies
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-ipafont-gothic \
    fonts-wqy-zenhei \
    fonts-thai-tlwg \
    fonts-khmeros \
    fonts-freefont-ttf \
    libxss1 \
    xvfb \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Set Chrome path for puppeteer
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV DISPLAY=:99

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY . .

# Start xvfb (virtual display) + server
CMD Xvfb :99 -screen 0 1280x900x24 &>/dev/null & node server.js
