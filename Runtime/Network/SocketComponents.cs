/*
* SPDX-License-Identifier: AGPL-3.0-or-later
* Copyright (C) 2025 Sergej Görzen <sergej.goerzen@gmail.com>
* This file is part of OmiLAXR.
*/
#nullable enable
using System;
using System.Buffers.Binary;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OmiLAXR.ReCoPa.Network
{
    /// <summary>
    /// Encodes and decodes event messages on a stream.
    /// </summary>
    public interface IFrameCodec
    {
        Task WriteAsync(Stream stream, string eventName, string payload, int maxMessageBytes, TimeSpan timeout, CancellationToken ct);
        Task<(string EventName, string Payload)> ReadAsync(Stream stream, int maxMessageBytes, TimeSpan timeout, CancellationToken ct);
    }

    /// <summary>
    /// Wait time before a reconnect attempt.
    /// </summary>
    public interface IReconnectPolicy
    {
        /// <param name="attempt">1-based reconnect attempt</param>
        TimeSpan GetDelay(int attempt);
    }

    /// <summary>
    /// Opens the byte stream to the ReCoPa server.
    /// </summary>
    public interface ISocketTransport
    {
        bool Connected { get; }

        /// <summary>Connects (closing a previous connection) and returns the stream.</summary>
        Task<Stream> ConnectAsync(string host, int port, int timeoutMs, CancellationToken ct);

        void Close();
    }

    /// <summary>
    /// ReCoPa wire format: [int32 bodyLen BE][uint16 eventLen BE][event UTF8][payload UTF8].
    /// </summary>
    public sealed class LengthPrefixedFrameCodec : IFrameCodec
    {
        public static readonly LengthPrefixedFrameCodec Instance = new LengthPrefixedFrameCodec();

        public async Task WriteAsync(Stream stream, string eventName, string payload, int maxMessageBytes, TimeSpan timeout, CancellationToken ct)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (eventName == null) throw new ArgumentNullException(nameof(eventName));
            payload ??= string.Empty;

            var evBytes = Encoding.UTF8.GetBytes(eventName);
            if (evBytes.Length > ushort.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(eventName), "eventName too long.");

            var plBytes = Encoding.UTF8.GetBytes(payload);

            var bodyLen = 2 + evBytes.Length + plBytes.Length;
            if (bodyLen <= 0 || bodyLen > maxMessageBytes)
                throw new InvalidDataException($"Message too large: {bodyLen} > {maxMessageBytes}");

            var frame = new byte[4 + bodyLen];
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), bodyLen);
            BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(4, 2), (ushort)evBytes.Length);

            Buffer.BlockCopy(evBytes, 0, frame, 6, evBytes.Length);
            Buffer.BlockCopy(plBytes, 0, frame, 6 + evBytes.Length, plBytes.Length);

            using var linked = CreateTimeoutCts(timeout, ct);
            await stream.WriteAsync(frame, 0, frame.Length, linked.Token).ConfigureAwait(false);
            await stream.FlushAsync(linked.Token).ConfigureAwait(false);
        }

        public async Task<(string EventName, string Payload)> ReadAsync(Stream stream, int maxMessageBytes, TimeSpan timeout, CancellationToken ct)
        {
            var lenBuf = new byte[4];
            await ReadExactAsync(stream, lenBuf, timeout, ct).ConfigureAwait(false);

            var bodyLen = BinaryPrimitives.ReadInt32BigEndian(lenBuf.AsSpan());
            if (bodyLen <= 0 || bodyLen > maxMessageBytes)
                throw new InvalidDataException($"Invalid body length {bodyLen} (limit {maxMessageBytes}).");

            var body = new byte[bodyLen];
            await ReadExactAsync(stream, body, timeout, ct).ConfigureAwait(false);

            var evLen = BinaryPrimitives.ReadUInt16BigEndian(body.AsSpan(0, 2));
            if (evLen == 0 || 2 + evLen > bodyLen)
                throw new InvalidDataException("Invalid event name length.");

            var ev = Encoding.UTF8.GetString(body, 2, evLen);
            var payload = Encoding.UTF8.GetString(body, 2 + evLen, bodyLen - (2 + evLen));
            return (ev, payload);
        }

        private static async Task ReadExactAsync(Stream stream, byte[] buffer, TimeSpan timeout, CancellationToken ct)
        {
            var offset = 0;
            using var linked = CreateTimeoutCts(timeout, ct);
            // NetworkStream.ReadAsync may ignore the token on some runtimes; closing the stream unblocks it.
            using var registration = linked.Token.Register(stream.Close);

            while (offset < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer, offset, buffer.Length - offset, linked.Token).ConfigureAwait(false);
                if (read == 0) throw new EndOfStreamException("Remote closed connection.");
                offset += read;
            }
        }

        private static CancellationTokenSource CreateTimeoutCts(TimeSpan timeout, CancellationToken ct)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            if (timeout > TimeSpan.Zero) cts.CancelAfter(timeout);
            return cts;
        }
    }

    /// <summary>
    /// Exponential backoff with +/- 10% jitter, configured by <see cref="SocketClientOptions"/>.
    /// </summary>
    public sealed class ExponentialBackoffReconnectPolicy : IReconnectPolicy
    {
        private static readonly object RandomLock = new();
        private static readonly Random SharedRandom = new();

        private readonly SocketClientOptions _opt;
        private readonly Func<double> _random;

        /// <param name="random">Source of values in [0, 1) for the jitter (default: shared Random)</param>
        public ExponentialBackoffReconnectPolicy(SocketClientOptions options, Func<double>? random = null)
        {
            _opt = options ?? throw new ArgumentNullException(nameof(options));
            _random = random ?? NextSharedRandom;
        }

        public TimeSpan GetDelay(int attempt)
        {
            // Compute exponential backoff based on attempt count.
            var factor = Math.Max(1.0, _opt.ReconnectBackoffFactor);
            double baseMs = Math.Max(0, _opt.ReconnectionDelay);
            var maxMs = Math.Max(baseMs, _opt.ReconnectionDelayMax);

            var ms = baseMs * Math.Pow(factor, Math.Max(0, attempt - 1));
            ms = Math.Min(ms, maxMs);

            // jitter +/- 10%
            var jitter = ms * 0.1;
            ms = ms + (_random() * 2.0 - 1.0) * jitter;

            return TimeSpan.FromMilliseconds(Math.Max(0, ms));
        }

        private static double NextSharedRandom()
        {
            lock (RandomLock) return SharedRandom.NextDouble();
        }
    }

    /// <summary>
    /// TCP connection configured by <see cref="SocketClientOptions"/> (NoDelay, keep-alive, connect timeout).
    /// </summary>
    public sealed class TcpSocketTransport : ISocketTransport
    {
        private readonly SocketClientOptions _opt;
        private TcpClient? _tcp;

        public TcpSocketTransport(SocketClientOptions options)
        {
            _opt = options ?? throw new ArgumentNullException(nameof(options));
        }

        public bool Connected => _tcp?.Connected == true;

        public async Task<Stream> ConnectAsync(string host, int port, int timeoutMs, CancellationToken ct)
        {
            // Ensure any previous socket is closed before reconnect.
            Close();

            // Create and configure the TCP client.
            var tcp = new TcpClient();
            ConfigureSocket(tcp);

            // Start connect attempt.
            var connectTask = tcp.ConnectAsync(host, port);

            // Enforce connection timeout when configured.
            if (timeoutMs > 0)
            {
                var timeoutTask = Task.Delay(timeoutMs, ct);
                var done = await Task.WhenAny(connectTask, timeoutTask).ConfigureAwait(false);
                if (done == timeoutTask)
                {
                    tcp.Close();
                    throw new TimeoutException($"Connect timeout after {timeoutMs}ms.");
                }
            }

            // Await completion and capture stream.
            await connectTask.ConfigureAwait(false);

            _tcp = tcp;
            return tcp.GetStream();
        }

        public void Close()
        {
            try { _tcp?.Close(); } catch { }
            _tcp = null;
        }

        private void ConfigureSocket(TcpClient tcp)
        {
            try
            {
                tcp.NoDelay = _opt.NoDelay;

                if (_opt.KeepAlive)
                {
                    tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

                    // OS-Level Keep-Alive Einstellungen (für Verbindungs-Stabilitaet)
                    try
                    {
                        // Linux/macOS: TCP Keep-Alive Tuning
                        #if !UNITY_EDITOR && !UNITY_STANDALONE_WIN
                        tcp.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.KeepAliveTime, _opt.TcpKeepAliveIdleSeconds);
                        tcp.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.KeepAliveInterval, _opt.TcpKeepAliveIntervalSeconds);
                        tcp.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.KeepAliveProbes, _opt.TcpKeepAliveProbes);
                        #endif
                    }
                    catch
                    {
                        // Einige Optionen sind ggf. nicht verfügbar - das ist ok
                    }
                }

                UnityEngine.Debug.Log($"[ReCoPa] Socket configured - NoDelay: {tcp.NoDelay}, KeepAlive: {_opt.KeepAlive}");
            }
            catch (System.Exception ex)
            {
                UnityEngine.Debug.LogWarning($"[ReCoPa] Socket configuration warning: {ex.Message}");
            }
        }
    }
}
