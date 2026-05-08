import WebSocket from "ws";
import { Packer } from "./Packer.js";

export class Client {
  constructor(opts = {}) {
    this.opts = {
      url: opts.url,
      packer: opts.packer || new Packer(opts.packerOptions || {}),
      heartbeat: opts.heartbeat || 0,

      // 默认 NodeID，发送消息时没传就用这个
      nodeID: opts.nodeID ?? 0,

      // 如果你的服务端心跳是普通消息，可以配置这个
      heartbeatMessageID: opts.heartbeatMessageID ?? 0,

      // request 默认超时时间
      requestTimeout: opts.requestTimeout ?? 10000,
    };

    this.websocket = null;
    this.intervalId = null;
    this.packer = this.opts.packer;

    this.connectHandler = null;
    this.disconnectHandler = null;
    this.receiveHandler = null;
    this.errorHandler = null;
    this.heartbeatHandler = null;

    // messageID -> Set(handler)
    this.messageHandlers = new Map();

    // key = messageID:seq
    this.waitgroup = new Map();

    this.seq = 0;
    this.closedByUser = false;
  }

  /**
   * 连接服务器
   * @returns {boolean}
   */
  connect() {
    try {
      this.disconnect();

      this.closedByUser = false;
      this.websocket = new WebSocket(this.opts.url);

      this.websocket.binaryType = "arraybuffer";

      this.websocket.on("open", () => {
        this.startHeartbeat();

        if (this.connectHandler) {
          this.connectHandler(this);
        }
      });

      this.websocket.on("close", (code, reason) => {
        this.stopHeartbeat();
        this.rejectAllPending(new Error("websocket closed"));

        if (this.disconnectHandler) {
          this.disconnectHandler(this, code, reason);
        }
      });

      this.websocket.on("error", (err) => {
        if (this.errorHandler) {
          this.errorHandler(this, err);
        }
      });

      this.websocket.on("message", (data) => {
        this.handleMessage(data);
      });

      return true;
    } catch (err) {
      console.error(err);
      return false;
    }
  }

  /**
   * 断开连接
   */
  disconnect() {
    this.closedByUser = true;

    this.stopHeartbeat();
    this.rejectAllPending(new Error("websocket disconnected"));

    if (!this.websocket) {
      return;
    }

    try {
      this.websocket.close();
    } catch (_) {
      // ignore
    }

    this.websocket = null;
  }

  /**
   * 启动心跳
   */
  startHeartbeat() {
    this.stopHeartbeat();

    if (!this.opts.heartbeat || this.opts.heartbeat <= 0) {
      return;
    }

    this.intervalId = setInterval(() => {
      if (!this.isConnected()) {
        return;
      }

      try {
        // 情况 1：如果 Packer 里实现了 packHeartbeat，就直接用
        if (typeof this.packer.packHeartbeat === "function") {
          const data = this.packer.packHeartbeat();
          this.websocket.send(data);
          return;
        }

        // 情况 2：如果你的服务端心跳是普通消息，就配置 heartbeatMessageID
        if (this.opts.heartbeatMessageID > 0) {
          this.send({
            nodeID: this.opts.nodeID,
            messageID: this.opts.heartbeatMessageID,
            seq: 0,
            buffer: new Uint8Array(0),
          });
        }
      } catch (err) {
        if (this.errorHandler) {
          this.errorHandler(this, err);
        }
      }
    }, this.opts.heartbeat);
  }

  /**
   * 停止心跳
   */
  stopHeartbeat() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  /**
   * 处理收到的消息
   * @param {ArrayBuffer|Buffer|Uint8Array} data
   */
  handleMessage(data) {
    if (!data || data.byteLength === 0) {
      return;
    }

    let packet;

    try {
      packet = this.packer.unpack(data);
    } catch (err) {
      if (this.errorHandler) {
        this.errorHandler(this, err);
      }
      return;
    }

    // 如果你的 Packer 支持心跳包
    if (packet.isHeartbeat) {
      if (this.heartbeatHandler) {
        this.heartbeatHandler(this, packet.millisecond);
      }
      return;
    }

    const message = packet.message;
    if (!message) {
      return;
    }

    // 优先处理 request 回调
    if (this.invoke(message)) {
      return;
    }

    // messageID 专属 handler
    const handlers = this.messageHandlers.get(message.messageID);
    if (handlers) {
      for (const handler of handlers) {
        try {
          handler(this, message);
        } catch (err) {
          if (this.errorHandler) {
            this.errorHandler(this, err);
          }
        }
      }
    }

    // 全局 receive handler
    if (this.receiveHandler) {
      this.receiveHandler(this, message);
    }
  }

  /**
   * 设置连接成功处理器
   * @param {(client: Client) => any} handler
   */
  onConnect(handler) {
    this.connectHandler = handler;
  }

  /**
   * 设置断开连接处理器
   * @param {(client: Client, code?: number, reason?: Buffer) => any} handler
   */
  onDisconnect(handler) {
    this.disconnectHandler = handler;
  }

  /**
   * 设置全局消息接收处理器
   * @param {(client: Client, message: Object) => any} handler
   */
  onReceive(handler) {
    this.receiveHandler = handler;
  }

  /**
   * 设置错误处理器
   * @param {(client: Client, err: Error) => any} handler
   */
  onError(handler) {
    this.errorHandler = handler;
  }

  /**
   * 设置心跳处理器
   * @param {(client: Client, millisecond?: number) => any} handler
   */
  onHeartbeat(handler) {
    this.heartbeatHandler = handler;
  }

  /**
   * 设置指定 messageID 的处理器
   * @param {number} messageID
   * @param {(client: Client, message: Object) => any} handler
   * @returns {Function} 取消监听函数
   */
  onMessage(messageID, handler) {
    if (!this.messageHandlers.has(messageID)) {
      this.messageHandlers.set(messageID, new Set());
    }

    this.messageHandlers.get(messageID).add(handler);

    return () => {
      this.offMessage(messageID, handler);
    };
  }

  /**
   * 移除指定 messageID 的处理器
   * @param {number} messageID
   * @param {Function} handler
   */
  offMessage(messageID, handler) {
    const handlers = this.messageHandlers.get(messageID);
    if (!handlers) {
      return;
    }

    handlers.delete(handler);

    if (handlers.size === 0) {
      this.messageHandlers.delete(messageID);
    }
  }

  /**
   * 检测客户端是否已连接
   * @returns {boolean}
   */
  isConnected() {
    return this.websocket !== null && this.websocket.readyState === WebSocket.OPEN;
  }

  /**
   * 检测客户端是否正在连接
   * @returns {boolean}
   */
  isConnecting() {
    return this.websocket !== null && this.websocket.readyState === WebSocket.CONNECTING;
  }

  /**
   * 发送消息
   *
   * message:
   * {
   *   nodeID: 1,
   *   messageID: 1001,
   *   seq: 1,
   *   buffer: Uint8Array
   * }
   *
   * 兼容写法：
   * {
   *   NodeID: 1,
   *   MessageID: 1001,
   *   Seq: 1,
   *   Buffer: Uint8Array
   * }
   *
   * @param {Object} message
   * @returns {boolean}
   */
  send(message) {
    if (!this.isConnected()) {
      return false;
    }

    const data = this.packer.packMessage({
      nodeID: message.nodeID ?? message.NodeID ?? this.opts.nodeID,
      messageID: message.messageID ?? message.MessageID,
      seq: message.seq ?? message.Seq ?? 0,
      buffer: message.buffer ?? message.Buffer ?? new Uint8Array(0),
    });

    this.websocket.send(data);

    return true;
  }

  /**
   * 请求 C/S 模型
   *
   * @param {number} messageID 消息 ID
   * @param {Uint8Array|Buffer|ArrayBuffer} buffer 消息体
   * @param {Object} options 配置
   * @returns {Promise<Object>}
   */
  request(messageID, buffer = new Uint8Array(0), options = {}) {
    return new Promise((resolve, reject) => {
      if (!this.isConnected()) {
        reject(new Error("websocket is not connected"));
        return;
      }

      const nodeID = options.nodeID ?? this.opts.nodeID;
      const timeout = options.timeout ?? this.opts.requestTimeout;
      const seq = options.seq ?? this.nextSeq();

      const key = this.makeWaitKey(messageID, seq);

      let timeoutId = null;

      if (timeout && timeout > 0) {
        timeoutId = setTimeout(() => {
          this.waitgroup.delete(key);
          reject(new Error(`request timeout, messageID=${messageID}, seq=${seq}`));
        }, timeout);
      }

      this.waitgroup.set(key, {
        resolve,
        reject,
        timeoutId,
      });

      const ok = this.send({
        nodeID,
        messageID,
        seq,
        buffer,
      });

      if (!ok) {
        if (timeoutId) {
          clearTimeout(timeoutId);
        }

        this.waitgroup.delete(key);
        reject(new Error("websocket send failed"));
      }
    });
  }

  /**
   * 调用 request 回调
   * @param {Object} message
   * @returns {boolean}
   */
  invoke(message) {
    const seq = message.seq ?? message.Seq ?? 0;
    const messageID = message.messageID ?? message.MessageID ?? 0;

    if (!seq || !messageID) {
      return false;
    }

    const key = this.makeWaitKey(messageID, seq);
    const waiter = this.waitgroup.get(key);

    if (!waiter) {
      return false;
    }

    this.waitgroup.delete(key);

    if (waiter.timeoutId) {
      clearTimeout(waiter.timeoutId);
    }

    waiter.resolve(message);

    return true;
  }

  /**
   * 生成 seq
   * @returns {number}
   */
  nextSeq() {
    this.seq++;

    if (this.seq > 2147483647) {
      this.seq = 1;
    }

    return this.seq;
  }

  /**
   * request 等待 key
   * @param {number} messageID
   * @param {number} seq
   * @returns {string}
   */
  makeWaitKey(messageID, seq) {
    return `${messageID}:${seq}`;
  }

  /**
   * 关闭时拒绝所有等待中的请求
   * @param {Error} err
   */
  rejectAllPending(err) {
    for (const [, waiter] of this.waitgroup) {
      if (waiter.timeoutId) {
        clearTimeout(waiter.timeoutId);
      }

      waiter.reject(err);
    }

    this.waitgroup.clear();
  }
}