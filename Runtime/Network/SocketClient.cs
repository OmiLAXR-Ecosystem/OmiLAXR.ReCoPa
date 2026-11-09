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

        private readonly ISocketTransport _transport;
        private readonly IFrameCodec _codec;
        private readonly IReconnectPolicy _reconnectPolicy;
        private Stream? _stream;

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
        public bool Connected => _stream != null && _transport.Connected;

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
            : this(connectionUrl, options, null, null, null)
        {
        }

        /// <summary>
        /// Creates a socket client with replaceable transport, wire format and reconnect timing (null = default).
        /// </summary>
        public SocketClient(string connectionUrl, SocketClientOptions options,
            ISocketTransport? transport, IFrameCodec? codec = null, IReconnectPolicy? reconnectPolicy = null)
        {
            _connectionUrl = connectionUrl ?? throw new ArgumentNullException(nameof(connectionUrl));
            _opt = options ?? throw new ArgumentNullException(nameof(options));
            _transport = transport ?? new TcpSocketTransport(_opt);
            _codec = codec ?? LengthPrefixedFrameCodec.Instance;
            _reconnectPolicy = reconnectPolicy ?? new ExponentialBackoffReconnectPolicy(_opt);

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
            _stream = await _transport.ConnectAsync(host, port, Math.Max(0, _opt.ConnectTimeoutMs), ct).ConfigureAwait(false);
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            if (_stream == null) throw new InvalidOperationException("No stream.");

            while (!ct.IsCancellationRequested)
            {
                // Read next framed message from the TCP stream.
                var (ev, payload) = await _codec.ReadAsync(
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
                await _codec.WriteAsync(
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

        private TimeSpan ComputeReconnectDelay(int attempt) => _reconnectPolicy.GetDelay(attempt);

        private void SafeClose()
        {
            try { _stream?.Close(); } catch { }
            try { _transport.Close(); } catch { }
            _stream = null;
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
    }
}
