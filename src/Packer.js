// Packer.js
// 对齐 Go 服务端格式：
// int32 totalLen
// int32 header / dataBit
// int32 nodeID
// int32 messageID
// int32 seq
// bytes payload

export const BIG_ENDIAN = "big";
export const LITTLE_ENDIAN = "little";

const DEFAULT_BYTE_ORDER = BIG_ENDIAN;

// 5 个 int32
const DEFAULT_HEADER_SIZE = 20;

// 你 Go 里面的 dataBit 如果不是 1，这里改成和服务端一致
export const DATA_BIT = 1;

const DEFAULT_BUFFER_BYTES = 5000;

const MAX_INT32 = 2147483647;
const MIN_INT32 = -2147483648;

function isLittleEndian(byteOrder) {
  return byteOrder === LITTLE_ENDIAN;
}

function checkInt32(name, value) {
  if (!Number.isInteger(value) || value < MIN_INT32 || value > MAX_INT32) {
    throw new Error(`${name} int32 overflow`);
  }
}

function toUint8Array(data) {
  if (!data) {
    return new Uint8Array(0);
  }

  if (data instanceof Uint8Array) {
    return data;
  }

  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }

  // Node.js Buffer 也是 Uint8Array，一般上面已经命中
  throw new Error("invalid buffer type");
}

function createDataView(bytes) {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

export class Packer {
  constructor(opts = {}) {
    this.byteOrder = opts.byteOrder || DEFAULT_BYTE_ORDER;
    this.bufferBytes = opts.bufferBytes ?? DEFAULT_BUFFER_BYTES;

    // 允许外部传入，避免 Go 侧 dataBit 不是 1 的情况
    this.dataBit = opts.dataBit ?? DATA_BIT;
  }

  /**
   * 打包普通消息
   *
   * message:
   * {
   *   gameID: 1,
   *   messageID: 1001,
   *   seq: 1,
   *   buffer: Uint8Array | ArrayBuffer | Buffer
   * }
   *
   * @returns {ArrayBuffer}
   */
  packMessage(message) {
    const gameID = message.gameID ?? message.GameID ?? 0;
    const messageID = message.messageID ?? message.MessageID ?? 0;
    const seq = message.seq ?? message.Seq ?? 0;
    const payload = toUint8Array(message.buffer ?? message.Buffer);

    if (payload.byteLength > this.bufferBytes) {
      throw new Error("message too large");
    }

    checkInt32("gameID", gameID);
    checkInt32("messageID", messageID);
    checkInt32("seq", seq);

    const totalLen = DEFAULT_HEADER_SIZE + payload.byteLength;

    if (totalLen > this.bufferBytes || totalLen > MAX_INT32) {
      throw new Error("message too large");
    }

    const arrayBuffer = new ArrayBuffer(totalLen);
    const bytes = new Uint8Array(arrayBuffer);
    const view = new DataView(arrayBuffer);
    const little = isLittleEndian(this.byteOrder);

    let offset = 0;

    // totalLen
    view.setInt32(offset, totalLen, little);
    offset += 4;

    // header / dataBit
    view.setInt32(offset, this.dataBit, little);
    offset += 4;

    // nodeID
    view.setInt32(offset, gameID, little);
    offset += 4;

    // messageID
    view.setInt32(offset, messageID, little);
    offset += 4;

    // seq
    view.setInt32(offset, seq, little);
    offset += 4;

    // payload
    bytes.set(payload, offset);

    return arrayBuffer;
  }

  /**
   * 解包单个完整包
   *
   * @param {ArrayBuffer|Uint8Array|Buffer} data
   * @returns {{
   *   totalLen: number,
   *   header: number,
   *   isData: boolean,
   *   message: {
   *     gameID: number,
   *     messageID: number,
   *     seq: number,
   *     buffer: Uint8Array
   *   }
   * }}
   */
  unpack(data) {
    const bytes = toUint8Array(data);

    if (bytes.byteLength < DEFAULT_HEADER_SIZE) {
      throw new Error("invalid message: header too small");
    }

    const view = createDataView(bytes);
    const little = isLittleEndian(this.byteOrder);

    let offset = 0;

    const totalLen = view.getInt32(offset, little);
    offset += 4;

    if (totalLen < DEFAULT_HEADER_SIZE) {
      throw new Error("invalid message: totalLen too small");
    }

    if (totalLen > this.bufferBytes) {
      throw new Error("message too large");
    }

    if (bytes.byteLength < totalLen) {
      throw new Error("invalid message: incomplete packet");
    }

    const header = view.getInt32(offset, little);
    offset += 4;

    const isData = (header & this.dataBit) === this.dataBit;

    if (!isData) {
      throw new Error("invalid message: dataBit not set");
    }

    const gameID = view.getInt32(offset, little);
    offset += 4;

    const messageID = view.getInt32(offset, little);
    offset += 4;

    const seq = view.getInt32(offset, little);
    offset += 4;

    const payloadLen = totalLen - DEFAULT_HEADER_SIZE;
    const buffer = bytes.subarray(offset, offset + payloadLen);

    return {
      totalLen,
      header,
      isData,
      message: {
        gameID,
        messageID,
        seq,
        buffer,
      },
    };
  }

  /**
   * 解包粘包数据。
   * WebSocket 一般不会粘包，但 TCP 流场景可以用这个。
   *
   * @param {ArrayBuffer|Uint8Array|Buffer} data
   * @returns {{ packets: Array, remain: Uint8Array }}
   */
  unpackFrames(data) {
    const bytes = toUint8Array(data);
    const little = isLittleEndian(this.byteOrder);

    const packets = [];
    let offset = 0;

    while (bytes.byteLength - offset >= DEFAULT_HEADER_SIZE) {
      const view = new DataView(
        bytes.buffer,
        bytes.byteOffset + offset,
        bytes.byteLength - offset,
      );

      const totalLen = view.getInt32(0, little);

      if (totalLen < DEFAULT_HEADER_SIZE) {
        throw new Error("invalid message: totalLen too small");
      }

      if (totalLen > this.bufferBytes) {
        throw new Error("message too large");
      }

      // 半包，留下次继续
      if (bytes.byteLength - offset < totalLen) {
        break;
      }

      const frame = bytes.subarray(offset, offset + totalLen);
      packets.push(this.unpack(frame));

      offset += totalLen;
    }

    return {
      packets,
      remain: bytes.subarray(offset),
    };
  }
}