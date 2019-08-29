FROM node:lts
WORKDIR /app

# Copy package files and install dependencies 
COPY package*.json ./
RUN npm install && npm install -g typescript

# Copy sources and compile
COPY . ./
RUN tsc

ENTRYPOINT ["node", "./consumePerf.js"]
