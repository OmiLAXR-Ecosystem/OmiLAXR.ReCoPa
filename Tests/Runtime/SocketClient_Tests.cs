using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using OmiLAXR.ReCoPa.Network;
using UnityEngine.TestTools;

namespace OmiLAXR.ReCoPa.Tests
{
    /// <summary>
    /// Exercises the ReCoPa wire protocol against a local TCP server:
    /// [int32 BE body length][uint16 BE event length][event UTF-8][payload UTF-8].
    /// </summary>
    public class SocketClient_Tests
    {
        private const int Timeout = 5000;

        private TcpListener _listener;
        private SocketClient _client;
        private readonly List<TcpClient> _accepted = new List<TcpClient>();

        [SetUp]
        public void SetUp()
        {
            // Connection errors are logged as errors by design when the server side closes.
            LogAssert.ignoreFailingMessages = true;
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
        }

        [TearDown]
        public void TearDown()
        {
            _client?.Dispose();
            foreach (var c in _accepted)
                c.Close();
            _accepted.Clear();
            _listener.Stop();
            LogAssert.ignoreFailingMessages = false;
        }

        private int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        private static SocketClientOptions Options(Action<SocketClientOptions> configure = null)
        {
            var options = new SocketClientOptions
            {
                UseSynchronizationContext = false,
                EnableHeartbeat = false,
                Reconnection = false,
                ConnectTimeoutMs = 2000,
                ReceiveTimeoutMs = 0,
            };
            configure?.Invoke(options);
            return options;
        }

        private SocketClient Connect(SocketClientOptions options, out NetworkStream server)
        {
            // Must be set inside the test body; the value set in SetUp does not apply to it.
            LogAssert.ignoreFailingMessages = true;
            _client = new SocketClient($"tcp://127.0.0.1:{Port}", options);
            var connected = new ManualResetEventSlim();
            _client.OnConnected += (_, __) => connected.Set();
            _client.ConnectAsync();
            server = Accept();
            Assert.IsTrue(connected.Wait(Timeout), "client did not connect");
            return _client;
        }

        private NetworkStream Accept()
        {
            var task = _listener.AcceptTcpClientAsync();
            Assert.IsTrue(task.Wait(Timeout), "no connection accepted");
            _accepted.Add(task.Result);
            var stream = task.Result.GetStream();
            stream.ReadTimeout = Timeout;
            return stream;
        }

        private static (string ev, string payload) ReadFrame(NetworkStream stream)
        {
            var header = ReadExact(stream, 4);
            var body = ReadExact(stream, BinaryPrimitives.ReadInt32BigEndian(header));
            var evLen = BinaryPrimitives.ReadUInt16BigEndian(body);
            return (Encoding.UTF8.GetString(body, 2, evLen), Encoding.UTF8.GetString(body, 2 + evLen, body.Length - 2 - evLen));
        }

        private static void WriteFrame(NetworkStream stream, string ev, string payload)
        {
            var evBytes = Encoding.UTF8.GetBytes(ev);
            var plBytes = Encoding.UTF8.GetBytes(payload);
            var frame = new byte[6 + evBytes.Length + plBytes.Length];
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), 2 + evBytes.Length + plBytes.Length);
            BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(4, 2), (ushort)evBytes.Length);
            evBytes.CopyTo(frame, 6);
            plBytes.CopyTo(frame, 6 + evBytes.Length);
            stream.Write(frame, 0, frame.Length);
        }

        private static byte[] ReadExact(NetworkStream stream, int count)
        {
            var buffer = new byte[count];
            var offset = 0;
            while (offset < count)
            {
                var read = stream.Read(buffer, offset, count - offset);
                if (read == 0) throw new Exception("connection closed");
                offset += read;
            }
            return buffer;
        }

        [Test]
        public void SocketClient_Emit_Writes_Framed_Json()
        {
            // Arrange
            var client = Connect(Options(), out var server);

            // Act
            client.EmitAsync("track", new { value = 42 }).Wait(Timeout);
            var (ev, payload) = ReadFrame(server);

            // Assert
            Assert.IsTrue(client.Connected);
            Assert.AreEqual("track", ev);
            Assert.AreEqual(42, (int)JObject.Parse(payload)["value"]);
        }

        [Test]
        public void SocketClient_Dispatches_Server_Events_To_Handlers()
        {
            // Arrange
            var client = Connect(Options(), out var server);
            var received = new ManualResetEventSlim();
            SocketResponse response = null;
            client.On("config", r => { response = r; received.Set(); });

            // Act
            WriteFrame(server, "unhandled", "{}");
            WriteFrame(server, "config", "{\"name\":\"Ünïcode\"}");

            // Assert
            Assert.IsTrue(received.Wait(Timeout));
            Assert.AreEqual("Ünïcode", (string)response.GetToken()["name"]);
            Assert.AreEqual("Ünïcode", response.GetValue<Dictionary<string, string>>()["name"]);
        }

        [Test]
        public void SocketClient_Handler_Exception_Raises_OnError()
        {
            // Arrange
            var client = Connect(Options(), out var server);
            var error = new ManualResetEventSlim();
            string message = null;
            client.OnError += (_, m) => { message = m; error.Set(); };
            client.On("boom", _ => throw new InvalidOperationException("handler failed"));

            // Act
            WriteFrame(server, "boom", "");

            // Assert
            Assert.IsTrue(error.Wait(Timeout));
            Assert.AreEqual("handler failed", message);
        }

        [Test]
        public void SocketClient_Buffers_Events_Until_Connected_And_Sends_Hello()
        {
            // Arrange
            _client = new SocketClient($"127.0.0.1:{Port}", Options(o =>
            {
                o.SessionId = "session-42";
                o.MaxBufferedMessages = 2;
            }));

            // Act
            _client.Emit("first", "1");
            _client.Emit("second", "2");
            _client.Emit("third", "3");
            _client.ConnectAsync();
            var server = Accept();

            // Assert (oldest message was dropped because of the buffer cap)
            Assert.AreEqual(("second", "2"), ReadFrame(server));
            Assert.AreEqual(("third", "3"), ReadFrame(server));
            var (ev, payload) = ReadFrame(server);
            Assert.AreEqual("hello", ev);
            Assert.AreEqual("session-42", (string)JObject.Parse(payload)["sessionId"]);
        }

        [Test]
        public void SocketClient_Emit_Without_Buffer_Throws_When_Disconnected()
        {
            // Arrange
            _client = new SocketClient($"127.0.0.1:{Port}", Options(o => o.BufferOutgoingWhenDisconnected = false));

            // Assert
            Assert.Throws<InvalidOperationException>(() => _client.EmitAsync("x", "y"));
        }

        [Test]
        public void SocketClient_Disposed_Client_Rejects_Calls()
        {
            // Arrange
            _client = new SocketClient($"127.0.0.1:{Port}", Options());

            // Act
            _client.Dispose();

            // Assert
            Assert.Throws<ObjectDisposedException>(() => _client.EmitAsync("x", "y"));
            Assert.Throws<ObjectDisposedException>(() => _client.ConnectAsync());
            Assert.DoesNotThrow(_client.Dispose);
        }

        [Test]
        public void SocketClient_Reports_Disconnect_When_Server_Closes()
        {
            // Arrange
            Connect(Options(), out _);
            var disconnected = new ManualResetEventSlim();
            _client.OnDisconnected += (_, __) => disconnected.Set();

            // Act
            _accepted[0].Close();

            // Assert
            Assert.IsTrue(disconnected.Wait(Timeout));
        }

        [Test]
        public void SocketClient_Reconnects_After_Server_Closes()
        {
            // Arrange
            Connect(Options(o =>
            {
                o.Reconnection = true;
                o.ReconnectionDelay = 50;
                o.ReconnectionDelayMax = 50;
            }), out _);
            var reconnected = new ManualResetEventSlim();
            var attempts = 0;
            _client.OnReconnected += (_, __) => reconnected.Set();
            _client.OnReconnectAttempt += (_, a) => attempts = a;

            // Act
            _accepted[0].Close();
            Accept();

            // Assert
            Assert.IsTrue(reconnected.Wait(Timeout));
            Assert.AreEqual(1, attempts);
        }

        [Test]
        public void SocketClient_Reconnects_After_Idle_Receive_Timeout()
        {
            // Arrange
            Connect(Options(o =>
            {
                o.Reconnection = true;
                o.ReconnectionDelay = 50;
                o.ReconnectionDelayMax = 50;
                o.ReceiveTimeoutMs = 200;
            }), out _);

            // Act (server stays silent longer than the receive timeout)
            var reconnect = _listener.AcceptTcpClientAsync();

            // Assert
            Assert.IsTrue(reconnect.Wait(Timeout), "client gave up instead of reconnecting");
            _accepted.Add(reconnect.Result);
        }

        [Test]
        public void SocketClient_Gives_Up_After_Max_Attempts()
        {
            // Arrange
            LogAssert.ignoreFailingMessages = true;
            _listener.Stop();
            var failed = new ManualResetEventSlim();
            _client = new SocketClient("127.0.0.1:1", Options(o =>
            {
                o.Reconnection = true;
                o.ReconnectionAttempts = 2;
                o.ReconnectionDelay = 10;
                o.ReconnectionDelayMax = 10;
            }));
            _client.OnReconnectFailed += (_, __) => failed.Set();

            // Act
            _client.ConnectAsync();

            // Assert
            Assert.IsTrue(failed.Wait(Timeout));
            _listener.Start();
        }

        [TestCase("http://example.org:5000", "example.org", 5000)]
        [TestCase("http://example.org", "example.org", 4567)]
        [TestCase("127.0.0.1:6000", "127.0.0.1", 6000)]
        [TestCase("localhost:6000", "localhost", 6000)]
        [TestCase("recopa-host", "recopa-host", 4567)]
        [TestCase("", "127.0.0.1", 4567)]
        public void SocketClient_Parses_Host_And_Port(string url, string host, int port)
        {
            // Act
            var result = ((string, int))typeof(SocketClient)
                .GetMethod("ParseHostPort", BindingFlags.NonPublic | BindingFlags.Static)
                .Invoke(null, new object[] { url });

            // Assert
            Assert.AreEqual((host, port), result);
        }

        [Test]
        public void SocketClient_Rejects_Null_Arguments()
        {
            Assert.Throws<ArgumentNullException>(() => new SocketClient(null, new SocketClientOptions()));
            Assert.Throws<ArgumentNullException>(() => new SocketClient("x", null));
            var client = new SocketClient("x", Options());
            Assert.Throws<ArgumentNullException>(() => client.On(null, _ => { }));
            Assert.Throws<ArgumentNullException>(() => client.On("x", null));
            Assert.Throws<ArgumentNullException>(() => client.JsonSerializer = null);
            client.Dispose();
        }
    }
}
