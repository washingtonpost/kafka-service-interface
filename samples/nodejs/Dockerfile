FROM node:latest

# Copy source and install app dependencies
RUN mkdir /src
WORKDIR /src
COPY . /src
RUN rm -rf /src/node_modules
RUN npm install

EXPOSE 3000

CMD node index.js