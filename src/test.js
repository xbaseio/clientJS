import { Client } from "./Client.js";
import { Packer } from "./Packer.js";

const client = new Client({
  url: "ws://127.0.0.1:3533",

  packer: new Packer({
    byteOrder: "big",
    bufferBytes: 5000,

    // 这个要和 Go 服务端 dataBit 一致
    dataBit: 1,
  }),

  // 心跳间隔，毫秒
  heartbeat: 10000,

  // 默认 NodeID
  nodeID: 1,

  // 如果服务端心跳是普通消息，填心跳 messageID
  // 如果 Packer 里有 packHeartbeat，可以不填
  heartbeatMessageID: 0,

  requestTimeout: 8000,
});

client.onConnect(() => {
  console.log("连接成功");

  const body = new TextEncoder().encode(JSON.stringify({
    userID: "10001",
    token: "abc",
  }));

  client.send({
    nodeID: 1,
    messageID: 1001,
    seq: 1,
    buffer: body,
  });
});

client.onDisconnect((c, code, reason) => {
  console.log("连接断开:", code, reason?.toString?.());
});

client.onError((c, err) => {
  console.error("连接错误:", err.message);
});

client.onReceive((c, message) => {
  console.log("收到消息:", message);

  if (message.buffer && message.buffer.length > 0) {
    const text = new TextDecoder().decode(message.buffer);
    console.log("消息体:", text);
  }
});

client.onMessage(2001, (c, message) => {
  console.log("收到 2001 推送:", message);
});

client.connect();