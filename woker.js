import { connect } from 'cloudflare:sockets';

const userID = 'b0763bec-687d-4185-b89f-85ed7b3f68b1';

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      
      // Giao diện web hiển thị chữ hi
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        const htmlContent = `
        <!DOCTYPE html>
        <html>
        <head>
          <meta charset="utf-8">
          <title>Trạng thái máy chủ</title>
          <style>
            body { font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #0f172a; color: #f8fafc; }
            .card { text-align: center; padding: 2rem; border-radius: 12px; background: #1e293b; box-shadow: 0 4px 20px rgba(0,0,0,0.4); }
            h1 { color: #22c55e; margin-bottom: 0.5rem; font-size: 2.5rem; }
            p { color: #94a3b8; margin: 0.2rem 0; }
          </style>
        </head>
        <body>
          <div class="card">
            <h1>hi! 👋</h1>
            <p><strong>Cloudflare Pages VLESS Active</strong></p>
            <p>Domain: ha.vouchergiare.store</p>
          </div>
        </body>
        </html>
        `;
        return new Response(htmlContent, {
          status: 200,
          headers: { 'Content-Type': 'text/html;charset=utf-8' },
        });
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
  let address = '';
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

      if (command !== 1) {
        webSocket.close();
        return;
      }

      let portIndex = 18 + optLength + 1;
      let port = new DataView(buffer.slice(portIndex, portIndex + 2)).getUint16(0);
      let addressIndex = portIndex + 2;
      let addressType = new Uint8Array(buffer.slice(addressIndex, addressIndex + 1))[0];

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

      remoteSocket = connect({ hostname: address, port: port });
      const writer = remoteSocket.writable.getWriter();
      await writer.write(rawData);
      writer.releaseLock();

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
