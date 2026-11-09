using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using OmiLAXR.ReCoPa.Network;
using UnityEngine.TestTools;

namespace OmiLAXR.ReCoPa.Tests
{
    public class SocketComponents_Tests
    {
        private const int Timeout = 5000;

        private sealed class FakeTransport : ISocketTransport
        {
            private readonly Func<Stream> _connect;
            public int Connects;
            public bool Connected { get; private set; }

            public FakeTransport(Func<Stream> connect) => _connect = connect;

            public Task<Stream> ConnectAsync(string host, int port, int timeoutMs, CancellationToken ct)
            {
                Connects++;
                var stream = _connect(); // may throw to simulate a refused connection
                Connected = true;
                return Task.FromResult(stream);
            }

            public void Close() => Connected = false;
        }

        private static SocketClientOptions Options(Action<SocketClientOptions> configure = null)
        {
            var options = new SocketClientOptions
            {
                UseSynchronizationContext = false,
                EnableHeartbeat = false,
                Reconnection = false,
                ReceiveTimeoutMs = 0,
            };
            configure?.Invoke(options);
            return options;
        }

        private static MemoryStream Frames(params (string ev, string payload)[] frames)
        {
            var stream = new MemoryStream();
            foreach (var (ev, payload) in frames)
                LengthPrefixedFrameCodec.Instance.WriteAsync(stream, ev, payload, 1024, TimeSpan.Zero, CancellationToken.None).Wait();
            stream.Position = 0;
            return stream;
        }

        [Test]
        public void FrameCodec_Round_Trips_Messages()
        {
            // Arrange
            var stream = Frames(("hello", "{\"a\":1}"), ("ünïcode", ""));

            // Act
            var first = LengthPrefixedFrameCodec.Instance.ReadAsync(stream, 1024, TimeSpan.Zero, CancellationToken.None).Result;
            var second = LengthPrefixedFrameCodec.Instance.ReadAsync(stream, 1024, TimeSpan.Zero, CancellationToken.None).Result;

            // Assert
            Assert.AreEqual(("hello", "{\"a\":1}"), first);
            Assert.AreEqual(("ünïcode", ""), second);
        }

        [Test]
        public void FrameCodec_Writes_Big_Endian_Length_Prefix()
        {
            // Act
            var bytes = Frames(("ab", "c")).ToArray();

            // Assert: body = 2 (event length) + 2 + 1 = 5
            CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 5, 0, 2, (byte)'a', (byte)'b', (byte)'c' }, bytes);
        }

        [Test]
        public void FrameCodec_Rejects_Invalid_Frames()
        {
            // Arrange
            var codec = LengthPrefixedFrameCodec.Instance;

            // Assert
            Assert.Throws<InvalidDataException>(() =>
                codec.WriteAsync(new MemoryStream(), "event", new string('x', 100), 10, TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult());
            Assert.Throws<InvalidDataException>(() =>
                codec.ReadAsync(new MemoryStream(new byte[] { 0, 0, 0, 0 }), 10, TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult());
            Assert.Throws<EndOfStreamException>(() =>
                codec.ReadAsync(new MemoryStream(new byte[] { 0, 0 }), 10, TimeSpan.Zero, CancellationToken.None).GetAwaiter().GetResult());
        }

        [Test]
        public void ReconnectPolicy_Uses_Capped_Exponential_Backoff()
        {
            // Arrange
            var options = Options(o =>
            {
                o.ReconnectionDelay = 1000;
                o.ReconnectionDelayMax = 3000;
                o.ReconnectBackoffFactor = 2;
            });
            var noJitter = new ExponentialBackoffReconnectPolicy(options, () => 0.5);
            var maxJitter = new ExponentialBackoffReconnectPolicy(options, () => 1.0);

            // Assert
            Assert.AreEqual(1000, noJitter.GetDelay(1).TotalMilliseconds, 1e-6);
            Assert.AreEqual(2000, noJitter.GetDelay(2).TotalMilliseconds, 1e-6);
            Assert.AreEqual(3000, noJitter.GetDelay(10).TotalMilliseconds, 1e-6);
            Assert.AreEqual(1100, maxJitter.GetDelay(1).TotalMilliseconds, 1e-6);
            Assert.Throws<ArgumentNullException>(() => new ExponentialBackoffReconnectPolicy(null));
        }

        [Test]
        public void SocketClient_Receives_Events_Through_Injected_Transport()
        {
            // Arrange
            var transport = new FakeTransport(() => Frames(("config", "{\"x\":1}")));
            var client = new SocketClient("127.0.0.1:1", Options(), transport);
            var received = new ManualResetEventSlim();
            var disconnected = new ManualResetEventSlim();
            string payload = null;
            client.On("config", r => { payload = r.RawText; received.Set(); });
            client.OnDisconnected += (_, __) => disconnected.Set();
            LogAssert.ignoreFailingMessages = true; // end of the fake stream is logged as connection error

            // Act
            client.ConnectAsync();

            // Assert
            Assert.IsTrue(received.Wait(Timeout));
            Assert.AreEqual("{\"x\":1}", payload);
            Assert.IsTrue(disconnected.Wait(Timeout), "end of stream disconnects");
            Assert.IsFalse(transport.Connected);
            client.Dispose();
        }

        [Test]
        public void SocketClient_Retries_Through_Policy_Until_Max_Attempts()
        {
            // Arrange
            var transport = new FakeTransport(() => throw new IOException("refused"));
            var delays = new List<int>();
            var client = new SocketClient("127.0.0.1:1", Options(o =>
            {
                o.Reconnection = true;
                o.ReconnectionAttempts = 2;
            }), transport, reconnectPolicy: new RecordingPolicy(delays));
            var failed = new ManualResetEventSlim();
            client.OnReconnectFailed += (_, __) => failed.Set();
            LogAssert.ignoreFailingMessages = true;

            // Act
            client.ConnectAsync();

            // Assert
            Assert.IsTrue(failed.Wait(Timeout));
            Assert.AreEqual(3, transport.Connects);
            CollectionAssert.AreEqual(new[] { 1, 2 }, delays);
            client.Dispose();
        }

        private sealed class RecordingPolicy : IReconnectPolicy
        {
            private readonly List<int> _attempts;
            public RecordingPolicy(List<int> attempts) => _attempts = attempts;

            public TimeSpan GetDelay(int attempt)
            {
                lock (_attempts)
                    _attempts.Add(attempt);
                return TimeSpan.Zero;
            }
        }
    }
}
