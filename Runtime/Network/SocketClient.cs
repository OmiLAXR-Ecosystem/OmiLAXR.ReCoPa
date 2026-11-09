/*
* SPDX-License-Identifier: AGPL-3.0-or-later
* Copyright (C) 2025 Sergej Görzen <sergej.goerzen@gmail.com>
* This file is part of OmiLAXR.
*/
#nullable enable
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace OmiLAXR.ReCoPa.Network
{
    /// <summary>
    /// Abstraction layer for JSON serialization and deserialization.
    /// Allows swapping the JSON backend without changing the socket client.
    /// </summary>
    public interface IJsonSerializer
    {
        /// <summary>
        /// Serialize a value to JSON.
        /// </summary>
        /// <param name="value">Value to serialize</param>
        /// <returns>JSON string</returns>
        string Serialize(object? value);

        /// <summary>
        /// Deserialize a JSON string into a strongly-typed value.
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="json">JSON input</param>
        /// <returns>Deserialized value</returns>
        T Deserialize<T>(string json);
    }

    /// <summary>
    /// Default JSON serializer backed by Newtonsoft.Json.
    /// </summary>
    public sealed class NewtonsoftJsonSerializer : IJsonSerializer
    {
        /// <summary>
        /// Serialize a value using Newtonsoft.Json semantics.
        /// </summary>
        /// <param name="value">Value to serialize</param>
        /// <returns>JSON string</returns>
        public string Serialize(object? value)
        {
            if (value == null) return string.Empty;
            if (value is string s) return s;
            if (value is JToken jt) return jt.ToString(Formatting.None);
            return JsonConvert.SerializeObject(value);
        }

        /// <summary>
        /// Deserialize JSON using Newtonsoft.Json.
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <param name="json">JSON input</param>
        /// <returns>Deserialized value</returns>
        public T Deserialize<T>(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                return default!;
            return JsonConvert.DeserializeObject<T>(json)!;
        }
    }
    
    

    // ------------------------------------------------------------
    // CLIENT: SocketClient (pure C#)
    // ------------------------------------------------------------
    /// <summary>
    /// TCP-based Socket.IO-like client for ReCoPa transport.
    /// Supports reconnects, heartbeat, and buffering for disconnected states.
    /// </summary>
    public partial class SocketClient : IDisposable
    {
        private readonly string _connectionUrl;
        private readonly SocketClientOptions _opt;

        private IJsonSerializer _serializer = new NewtonsoftJsonSerializer();

        private readonly object _gate = new();
        // Serializes frame writes; heartbeat and caller threads may emit concurrently.
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly Dictionary<string, List<Action<SocketResponse>>> _handlers = new(StringComparer.Ordinal);

        private TcpClient? _tcp;
        private NetworkStream? _stream;

        private CancellationTokenSource? _cts;
        private Task? _runTask;
        private Task? _heartbeatTask;

        private bool _disposed;
        private bool _everConnected;
        private int _attempt;
        private bool _heartbeatReceived = true;
        private bool _heartbeatRunning;
        private readonly List<(string EventName, string Payload)> _outgoingQueue = new();

        private SynchronizationContext? _syncContext;

        // SocketIOUnity-like events
        /// <summary>
        /// Fired when the client connects for the first time.
        /// </summary>
        public event EventHandler? OnConnected;

        /// <summary>
        /// Fired when the client reconnects after a disconnect.
        /// </summary>
        public event EventHandler? OnReconnected;

        /// <summary>
        /// Fired when the client disconnects.
        /// </summary>
        public event EventHandler? OnDisconnected;

        /// <summary>
        /// Fired for each reconnect attempt with the attempt count.
        /// </summary>
        public event EventHandler<int>? OnReconnectAttempt;

        /// <summary>
        /// Fired when a reconnect attempt fails with an exception.
        /// </summary>
        public event EventHandler<Exception>? OnReconnectError;

        /// <summary>
        /// Fired when reconnect attempts are exhausted.
        /// </summary>
        public event EventHandler? OnReconnectFailed;

        /// <summary>
        /// Fired on non-fatal client errors.
        /// </summary>
        public event EventHandler<string>? OnError;

        /// <summary>
        /// True when the underlying TCP client is connected.
        /// </summary>
        public bool Connected => _tcp?.Connected == true;

        /// <summary>
        /// Gets or sets the JSON serializer used for payloads.
        /// </summary>
        public IJsonSerializer JsonSerializer
        {
            get => _serializer;
            set => _serializer = value ?? throw new ArgumentNullException(nameof(value));
        }

        /// <summary>
        /// Creates a socket client for the given connection URL and options.
        /// </summary>
        /// <param name="connectionUrl">Target host:port or URL</param>
        /// <param name="options">Client configuration options</param>
        public SocketClient(string connectionUrl, SocketClientOptions options)
        {
            _connectionUrl = connectionUrl ?? throw new ArgumentNullException(nameof(connectionUrl));
            _opt = options ?? throw new ArgumentNullException(nameof(options));

            _syncContext = (_opt.UseSynchronizationContext ? SynchronizationContext.Current : null);
        }

        // Like SocketIOUnity.On("event", cb)
        /// <summary>
        /// Registers a callback for a named event.
        /// </summary>
        /// <param name="eventName">Event name</param>
        /// <param name="callback">Callback invoked with the response</param>
        public void On(string eventName, Action<SocketResponse> callback)
        {
            if (eventName == null) throw new ArgumentNullException(nameof(eventName));
            if (callback == null) throw new ArgumentNullException(nameof(callback));

            lock (_gate)
            {
                if (!_handlers.TryGetValue(eventName, out var list))
                {
                    list = new List<Action<SocketResponse>>();
                    _handlers[eventName] = list;
                }
                list.Add(callback);
            }
        }

        // Like SocketIOUnity.Emit / EmitAsync
        /// <summary>
        /// Emits an event with a payload (fire-and-forget).
        /// </summary>
        /// <param name="eventName">Event name</param>
        /// <param name="data">Payload object</param>
        public void Emit(string eventName, object data) => _ = EmitAsync(eventName, data);

        /// <summary>
        /// Emits an event asynchronously with a payload.
        /// </summary>
        /// <param name="eventName">Event name</param>
        /// <param name="data">Payload object</param>
        /// <returns>Task that completes when the payload is queued or sent</returns>
        public Task EmitAsync(string eventName, object data)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(SocketClient));

            var payload = _serializer.Serialize(data);
            //UnityEngine.Debug.Log($"Emitting event {eventName} with payload {payload}");

            // If connected, send immediately
            if (Connected && _stream != null)
            {
                return WriteFrameAsync(eventName, payload, _cts?.Token ?? CancellationToken.None);
            }

            // If buffering is enabled, enqueue and return completed task
            if (_opt.BufferOutgoingWhenDisconnected)
            {
                lock (_gate)
                {
                    _outgoingQueue.Add((eventName, payload));
                    if (_outgoingQueue.Count > Math.Max(1, _opt.MaxBufferedMessages))
                    {
                        // drop oldest
                        _outgoingQueue.RemoveAt(0);
                    }
                }
                UnityEngine.Debug.Log($"[ReCoPa] Buffered event '{eventName}', queue size now {_outgoingQueue.Count}");
                return Task.CompletedTask;
            }

            // Default: throw when not connected (legacy behaviour)
            throw new InvalidOperationException("SocketClient is not connected.");
        }

        // Like SocketIOUnity.ConnectAsync()
        /// <summary>
        /// Starts the client connection and background run loop.
        /// </summary>
        /// <returns>Completed task once the run loop is started</returns>
        public Task ConnectAsync()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(SocketClient));
            if (_runTask != null) return Task.CompletedTask;

            // Create cancellation scope for background loops.
            _cts = new CancellationTokenSource();

            // Capture current sync context for main-thread dispatch when requested.
            if (_opt.UseSynchronizationContext && _syncContext == null)
                _syncContext = SynchronizationContext.Current;

            // Heartbeat Handler registrieren
            if (_opt.EnableHeartbeat)
            {
                On("heartbeat:ping", resp => HandleHeartbeatPing());
            }

            // ensure outgoing messages are handled when we reconnect

            // Start connection loop on a background task.
            _runTask = Task.Run(() => RunLoopAsync(_cts.Token));
            return Task.CompletedTask;
        }

        /// <summary>
        /// Disconnects the client and stops background tasks.
        /// </summary>
        public void Disconnect()
        {
            if (_disposed) return;
            try { _cts?.Cancel(); } catch { }
            SafeClose();
        }

        /// <summary>
        /// Disposes the client and releases all resources.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            Disconnect();
            _heartbeatRunning = false;
            try { _cts?.Dispose(); } catch { }
            _cts = null;
            _runTask = null;
            _heartbeatTask = null;
        }
        
        /// <summary>
        /// Main connection loop that handles connect, receive, disconnect, and reconnect.
        /// </summary>
        /// <param name="ct">Cancellation token to stop the loop</param>
        private async Task RunLoopAsync(CancellationToken ct)
        {
            // Resolve host/port once from the configured URL.
            var (host, port) = ParseHostPort(_connectionUrl);

            // Keep trying until cancelled or reconnection is disabled.
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    // Establish a single connection attempt.
                    await ConnectOnceAsync(host, port, ct).ConfigureAwait(false);

                    // Flush any queued outgoing messages after connect.
                    try
                    {
                        await FlushOutgoingQueueAsync(ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        UnityEngine.Debug.LogWarning($"[ReCoPa] FlushOutgoingQueue failed: {ex.Message}");
                    }

                    // TCP has no headers -> send once as hello event.
                    if ((_opt.ExtraHeaders != null && _opt.ExtraHeaders.Count > 0) || !string.IsNullOrWhiteSpace(_opt.SessionId))
                    {
                        var helloObj = new JObject
                        {
                            ["headers"] = JObject.FromObject(_opt.ExtraHeaders),
                            ["sessionId"] = _opt.SessionId ?? string.Empty,
                            ["ts"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                        };
                        await EmitAsync("hello", helloObj).ConfigureAwait(false);
                    }

                    // Fire connected/reconnected callbacks.
                    if (_everConnected) RaiseOnContext(() => OnReconnected?.Invoke(this, EventArgs.Empty));
                    else RaiseOnContext(() => OnConnected?.Invoke(this, EventArgs.Empty));

                    // Reset connection state markers.
                    _everConnected = true;
                    _attempt = 0;
                    _heartbeatReceived = true;

                    // Starte Heartbeat wenn aktiviert
                    if (_opt.EnableHeartbeat && !_heartbeatRunning)
                    {
                        _heartbeatRunning = true;
                        _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(ct), ct);
                    }

                    // Enter blocking receive loop until disconnect.
                    await ReceiveLoopAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    // Only a real cancellation ends the loop; receive/send timeouts reconnect below.
                    break;
                }
                catch (Exception ex)
                {
                    if (ct.IsCancellationRequested || _disposed)
                        break;
                    // Report non-fatal connection errors.
                    UnityEngine.Debug.LogError($"[ReCoPa] Connection error: {ex.GetType().Name}: {ex.Message}");
                    RaiseOnContext(() => OnError?.Invoke(this, ex.Message));
                    RaiseOnContext(() => OnReconnectError?.Invoke(this, ex));
                }
                finally
                {
                    // Always cleanup socket and notify disconnection.
                    _heartbeatRunning = false;
                    SafeClose();
                    RaiseOnContext(() => OnDisconnected?.Invoke(this, EventArgs.Empty));
                }

                // Exit if we should not reconnect.
                if (ct.IsCancellationRequested) break;
                if (!_opt.Reconnection) break;

                // Increment attempt and notify listeners.
                _attempt++;
                RaiseOnContext(() => OnReconnectAttempt?.Invoke(this, _attempt));

                // Unlimited retries wenn ReconnectionAttempts <= 0
                if (_opt.ReconnectionAttempts > 0 && _attempt > _opt.ReconnectionAttempts)
                {
                    RaiseOnContext(() => OnReconnectFailed?.Invoke(this, EventArgs.Empty));
                    break;
                }

                // Wait before next reconnect attempt.
                var delay = ComputeReconnectDelay(_attempt);
                UnityEngine.Debug.Log($"[ReCoPa] Reconnect attempt {_attempt}, waiting {delay.TotalSeconds:F1}s");
                try { await Task.Delay(delay, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }

        private async Task ConnectOnceAsync(string host, int port, CancellationToken ct)
        {
            // Ensure any previous socket is closed before reconnect.
            SafeClose();

            // Create and configure the TCP client.
            var tcp = new TcpClient();
            ConfigureSocket(tcp);

            // Start connect attempt.
            var connectTask = tcp.ConnectAsync(host, port);

            // Enforce connection timeout when configured.
            var timeout = Math.Max(0, _opt.ConnectTimeoutMs);
            if (timeout > 0)
            {
                var timeoutTask = Task.Delay(timeout, ct);
                var done = await Task.WhenAny(connectTask, timeoutTask).ConfigureAwait(false);
                if (done == timeoutTask)
                    throw new TimeoutException($"Connect timeout after {timeout}ms.");
            }

            // Await completion and capture stream.
            await connectTask.ConfigureAwait(false);

            _tcp = tcp;
            _stream = tcp.GetStream();
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            if (_stream == null) throw new InvalidOperationException("No stream.");

            while (!ct.IsCancellationRequested)
            {
                // Read next framed message from the TCP stream.
                var (ev, payload) = await Framing.ReadMessageAsync(
                    _stream,
                    _opt.MaxMessageBytes,
                    TimeSpan.FromMilliseconds(Math.Max(0, _opt.ReceiveTimeoutMs)),
                    ct
                ).ConfigureAwait(false);

                // Dispatch to handlers.
                Dispatch(ev, payload);
            }
        }

        // Flush outgoing queue: send all buffered messages in order
        private async Task FlushOutgoingQueueAsync(CancellationToken ct)
        {
            List<(string EventName, string Payload)> toSend;
            lock (_gate)
            {
                // Nothing buffered -> no work to do.
                if (_outgoingQueue.Count == 0) return;
                toSend = new List<(string, string)>(_outgoingQueue);
                _outgoingQueue.Clear();
            }

            for (var i = 0; i < toSend.Count; i++)
            {
                if (ct.IsCancellationRequested) break;
                var (ev, payload) = toSend[i];
                try
                {
                    // Send buffered message with normal framing and timeouts.
                    await WriteFrameAsync(ev, payload, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    UnityEngine.Debug.LogWarning($"[ReCoPa] Failed to send buffered event '{ev}': {ex.Message}");
                    // If a send fails, put the failed and all remaining messages back in their original order
                    lock (_gate)
                    {
                        _outgoingQueue.InsertRange(0, toSend.Skip(i));
                    }
                    throw;
                }
            }
        }

        private async Task WriteFrameAsync(string eventName, string payload, CancellationToken ct)
        {
            await _writeLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var stream = _stream ?? throw new InvalidOperationException("No stream.");
                await Framing.WriteMessageAsync(
                    stream,
                    eventName,
                    payload,
                    _opt.MaxMessageBytes,
                    TimeSpan.FromMilliseconds(Math.Max(0, _opt.SendTimeoutMs)),
                    ct
                ).ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        private void Dispatch(string eventName, string payload)
        {
            List<Action<SocketResponse>>? list;

            lock (_gate)
            {
                // Snapshot handler list under lock to avoid concurrent modification.
                _handlers.TryGetValue(eventName, out list);
                list = list == null ? null : new List<Action<SocketResponse>>(list);
            }

            if (list == null) return;

            // Create response wrapper for typed deserialization.
            var resp = new SocketResponse(payload, _serializer);

            foreach (var cb in list)
            {
                try { cb(resp); }
                catch (Exception ex) { RaiseOnContext(() => OnError?.Invoke(this, ex.Message)); }
            }
        }

        protected void RaiseOnContext(Action a)
        {
            if (!_opt.UseSynchronizationContext || _syncContext == null)
            {
                // No sync context: run directly on current thread.
                a();
                return;
            }
            // Marshal back to captured synchronization context.
            _syncContext.Post(_ => a(), null);
        }

        private TimeSpan ComputeReconnectDelay(int attempt)
        {
            // Compute exponential backoff based on attempt count.
            var factor = Math.Max(1.0, _opt.ReconnectBackoffFactor);
            double baseMs = Math.Max(0, _opt.ReconnectionDelay);
            var maxMs = Math.Max(baseMs, _opt.ReconnectionDelayMax);

            var ms = baseMs * Math.Pow(factor, Math.Max(0, attempt - 1));
            ms = Math.Min(ms, maxMs);

            // jitter +/- 10%
            var jitter = ms * 0.1;
            var r = ThreadSafeRandom.NextDouble();
            ms = ms + (r * 2.0 - 1.0) * jitter;

            return TimeSpan.FromMilliseconds(Math.Max(0, ms));
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
            catch (Exception ex) 
            { 
                UnityEngine.Debug.LogWarning($"[ReCoPa] Socket configuration warning: {ex.Message}");
            }
        }

        private void SafeClose()
        {
            try { _stream?.Close(); } catch { }
            try { _tcp?.Close(); } catch { }
            _stream = null;
            _tcp = null;
        }

        // ============================================================
        // Heartbeat System - Staebiltaet durch regelmäßige Pings
        // ============================================================
        private async Task HeartbeatLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested && _heartbeatRunning)
                {
                    try
                    {
                        // Warte bis zum nächsten Heartbeat-Intervall
                        await Task.Delay(_opt.HeartbeatIntervalMs, ct).ConfigureAwait(false);

                        // Prüfe ob noch verbunden
                        if (_stream == null || !Connected)
                        {
                            UnityEngine.Debug.LogWarning($"[ReCoPa] Heartbeat: Stream ist null oder disconnected");
                            break;
                        }

                        // Reset Pong-Flag und sende Ping um die Verbindung am Leben zu halten
                        _heartbeatReceived = false;
                        
                        try
                        {
                            // Sende Ping - dies faellt auf je if Stream nicht mehr writable ist
                            await EmitAsync("heartbeat:pong", new { ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() })
                                .ConfigureAwait(false);
                            
                            UnityEngine.Debug.Log($"[ReCoPa] Heartbeat sent ♥");
                        }
                        catch (Exception ex)
                        {
                            UnityEngine.Debug.LogWarning($"[ReCoPa] Failed to send heartbeat: {ex.GetType().Name}");
                            // Heartbeat-Send-Fehler = Verbindung ist kaputt
                            throw;
                        }

                        // Warte auf Pong-Response mit Timeout
                        var timeout = _opt.HeartbeatTimeoutMs;
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        var maxWaitTime = Math.Min(timeout, _opt.HeartbeatIntervalMs / 2);

                        while (sw.ElapsedMilliseconds < maxWaitTime && !_heartbeatReceived && _heartbeatRunning && !ct.IsCancellationRequested)
                        {
                            await Task.Delay(50, ct).ConfigureAwait(false);
                        }

                        if (!_heartbeatReceived && _heartbeatRunning)
                        {
                            UnityEngine.Debug.LogWarning($"[ReCoPa] Heartbeat timeout (>{maxWaitTime}ms ohne Pong) - forcing reconnect");
                            throw new TimeoutException($"Heartbeat timeout - no pong within {maxWaitTime}ms");
                        }

                        if (_heartbeatReceived)
                        {
                            UnityEngine.Debug.Log($"[ReCoPa] Heartbeat OK ✓");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        UnityEngine.Debug.LogError($"[ReCoPa] Heartbeat error: {ex.GetType().Name}: {ex.Message}");
                        // Heartbeat-Fehler = Reconnect triggern: nobody observes this task, so closing the
                        // socket is what makes the receive loop fail and the run loop reconnect.
                        SafeClose();
                        break;
                    }
                }
            }
            finally
            {
                _heartbeatRunning = false;
            }
        }

        private void HandleHeartbeatPing()
        {
            _heartbeatReceived = true;
        }

        private static (string host, int port) ParseHostPort(string urlOrHostPort)
        {
            const int defaultPort = 4567;

            if (string.IsNullOrWhiteSpace(urlOrHostPort))
                return ("127.0.0.1", defaultPort);

            // "localhost:4567" is a valid absolute URI with scheme "localhost" and no host, so require a host
            if (Uri.TryCreate(urlOrHostPort, UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Host))
            {
                var host = uri.Host;
                var port = uri.IsDefaultPort ? defaultPort : uri.Port;
                return (host, port);
            }

            var parts = urlOrHostPort.Split(':');
            if (parts.Length == 2 && int.TryParse(parts[1], out var p))
                return (parts[0], p);

            return (urlOrHostPort, defaultPort);
        }

        // ------------------------------------------------------------
        // Framing: [int32 bodyLen BE][uint16 eventLen BE][event UTF8][payload UTF8]
        // ------------------------------------------------------------
        private static class Framing
        {
            public static async Task WriteMessageAsync(
                NetworkStream stream,
                string eventName,
                string payload,
                int maxMessageBytes,
                TimeSpan sendTimeout,
                CancellationToken ct)
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

                using var linked = CreateTimeoutCts(sendTimeout, ct);
                await stream.WriteAsync(frame, 0, frame.Length, linked.Token).ConfigureAwait(false);
                await stream.FlushAsync(linked.Token).ConfigureAwait(false);
            }

            public static async Task<(string EventName, string Payload)> ReadMessageAsync(
                NetworkStream stream,
                int maxMessageBytes,
                TimeSpan receiveTimeout,
                CancellationToken ct)
            {
                var lenBuf = new byte[4];
                await ReadExactAsync(stream, lenBuf, receiveTimeout, ct).ConfigureAwait(false);

                var bodyLen = BinaryPrimitives.ReadInt32BigEndian(lenBuf.AsSpan());
                if (bodyLen <= 0 || bodyLen > maxMessageBytes)
                    throw new InvalidDataException($"Invalid body length {bodyLen} (limit {maxMessageBytes}).");

                var body = new byte[bodyLen];
                await ReadExactAsync(stream, body, receiveTimeout, ct).ConfigureAwait(false);

                var evLen = BinaryPrimitives.ReadUInt16BigEndian(body.AsSpan(0, 2));
                if (evLen == 0 || 2 + evLen > bodyLen)
                    throw new InvalidDataException("Invalid event name length.");

                var ev = Encoding.UTF8.GetString(body, 2, evLen);
                var payload = Encoding.UTF8.GetString(body, 2 + evLen, bodyLen - (2 + evLen));
                return (ev, payload);
            }

            private static async Task ReadExactAsync(NetworkStream stream, byte[] buffer, TimeSpan timeout, CancellationToken ct)
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

        private static class ThreadSafeRandom
        {
            private static readonly object _lock = new();
            private static readonly Random _rnd = new();

            public static double NextDouble()
            {
                lock (_lock) return _rnd.NextDouble();
            }
        }
    }
}
