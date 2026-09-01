import { connect } from 'cloudflare:sockets';

const userID = 'b0763bec-687d-4185-b89f-85ed7b3f68b1';
const proxyIPs = ['cdn.xn--b6gac.eu.org', 'cdn-all.xn--b6gac.eu.org', 'edgetunnel.anycast.eu.org'];

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        const html = `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>hi</title></head>
<body style="background:#0f172a;color:#fff;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;font-family:sans-serif;">
  <div style="text-align:center;padding:20px;background:#1e293b;border-radius:8px;">
    <h1 style="color:#22c55e;margin:0;font-size:3rem;">hi! 👋</h1>
    <p style="color:#94a3b8;margin-top:8px;">Worker VLESS Online (Port 80)</p>
  </div>
</body>
</html>`;
        return new Response(html, {
          status: 200,
          headers: { 'Content-Type': 'text/html;charset=utf-8' }
        });
      }

      const webSocketPair = new WebSocketPair();
      const [client, server] = Object.values(webSocketPair);
      server.accept();

      handleVLESS(server);

      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function handleVLESS(webSocket) {
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
      const optLength = new Uint8Array(buffer.slice(17, 18))[0];
      const command = new Uint8Array(buffer.slice(18 + optLength, 18 + optLength + 1))[0];

      if (command !== 1) {
        webSocket.close();
        return;
      }

      const portIndex = 18 + optLength + 1;
      const port = new DataView(buffer.slice(portIndex, portIndex + 2)).getUint16(0);
      const addressIndex = portIndex + 2;
      const addressType = new Uint8Array(buffer.slice(addressIndex, addressIndex + 1))[0];

      let address = '';
      let headerLength = 0;

      if (addressType === 1) {
        address = new Uint8Array(buffer.slice(addressIndex + 1, addressIndex + 5)).join('.');
        headerLength = addressIndex + 5;
      } else if (addressType === 2) {
        const domainLength = new Uint8Array(buffer.slice(addressIndex + 1, addressIndex + 2))[0];
        address = new TextDecoder().decode(buffer.slice(addressIndex + 2, addressIndex + 2 + domainLength));
        headerLength = addressIndex + 2 + domainLength;
      } else if (addressType === 3) {
        const ipv6Data = new DataView(buffer.slice(addressIndex + 1, addressIndex + 17));
        const ipv6 = [];
        for (let i = 0; i < 8; i++) {
          ipv6.push(ipv6Data.getUint16(i * 2).toString(16));
        }
        address = ipv6.join(':');
        headerLength = addressIndex + 17;
      }

      const rawData = buffer.slice(headerLength);
      webSocket.send(new Uint8Array([version[0], 0]));

      const targetProxy = proxyIPs[Math.floor(Math.random() * proxyIPs.length)];

      try {
        remoteSocket = connect({ hostname: address, port: port });
        const writer = remoteSocket.writable.getWriter();
        await writer.write(rawData);
        writer.releaseLock();
      } catch (e) {
        remoteSocket = connect({ hostname: targetProxy, port: port });
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
