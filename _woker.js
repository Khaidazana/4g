import { connect } from 'cloudflare:sockets';

const userID = 'b0763bec-687d-4185-b89f-85ed7b3f68b1';
// Proxy IP dự phòng của Cloudflare để xử lý outbounds
const proxyIP = 'cdn-all.xn--b6gac.eu.org'; 

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        return env.ASSETS ? env.ASSETS.fetch(request) : new Response('hi! 👋 Active', { status: 200 });
      }

      const webSocketPair = new WebSocketPair();
      const [client, server] = Object.values(webSocketPair);
      server.accept();

      handleSession(server, userID);

      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function handleSession(webSocket, userID) {
  let remoteSocket = null;

  webSocket.addEventListener('message', async (event) => {
    try {
      if (remoteSocket) {
        const writer = remoteSocket.writable.getWriter();
        await writer.write(event.data);
        writer.releaseLock();
        return;
      }

      const buffer = event.data;
      if (buffer.byteLength < 18) return;

      const version = new Uint8Array(buffer.slice(0, 1));
      let optLength = new Uint8Array(buffer.slice(17, 18))[0];
      let command = new Uint8Array(buffer.slice(18 + optLength, 18 + optLength + 1))[0];

      if (command !== 1) { // 1 = TCP
        webSocket.close();
        return;
      }

      let portIndex = 18 + optLength + 1;
      let port = new DataView(buffer.slice(portIndex, portIndex + 2)).getUint16(0);
      let addressIndex = portIndex + 2;
      let addressType = new Uint8Array(buffer.slice(addressIndex, addressIndex + 1))[0];

      let address = '';
      let headerLength = 0;

      if (addressType === 1) {
        address = new Uint8Array(buffer.slice(addressIndex + 1, addressIndex + 5)).join('.');
        headerLength = addressIndex + 5;
      } else if (addressType === 2) {
        let domainLength = new Uint8Array(buffer.slice(addressIndex + 1, addressIndex + 2))[0];
        address = new TextDecoder().decode(buffer.slice(addressIndex + 2, addressIndex + 2 + domainLength));
        headerLength = addressIndex + 2 + domainLength;
      } else if (addressType === 3) {
        let ipv6Data = new DataView(buffer.slice(addressIndex + 1, addressIndex + 17));
        let ipv6 = [];
        for (let i = 0; i < 8; i++) {
          ipv6.push(ipv6Data.getUint16(i * 2).toString(16));
        }
        address = ipv6.join(':');
        headerLength = addressIndex + 17;
      }

      const rawData = buffer.slice(headerLength);
      webSocket.send(new Uint8Array([version[0], 0]));

      // Thử kết nối trực tiếp, nếu Cloudflare chặn sẽ đẩy qua ProxyIP
      try {
        remoteSocket = connect({ hostname: address, port: port });
        const writer = remoteSocket.writable.getWriter();
        await writer.write(rawData);
        writer.releaseLock();
      } catch (err) {
        remoteSocket = connect({ hostname: proxyIP, port: port });
        const writer = remoteSocket.writable.getWriter();
        await writer.write(rawData);
        writer.releaseLock();
      }

      remoteSocket.readable.pipeTo(
        new WritableStream({
          write(chunk) {
            webSocket.send(chunk);
          },
          close() {
            webSocket.close();
          },
          abort() {
            webSocket.close();
          },
        })
      ).catch(() => {
        webSocket.close();
      });
    } catch (e) {
      webSocket.close();
    }
  });

  webSocket.addEventListener('close', () => {
    if (remoteSocket) remoteSocket.close();
  });

  webSocket.addEventListener('error', () => {
    if (remoteSocket) remoteSocket.close();
  });
}
