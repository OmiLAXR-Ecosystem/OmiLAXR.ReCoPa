/*
* SPDX-License-Identifier: AGPL-3.0-or-later
* Copyright (C) 2025 Sergej Görzen <sergej.goerzen@gmail.com>
* This file is part of OmiLAXR.
*/
using System;
using Newtonsoft.Json.Linq;

namespace OmiLAXR.ReCoPa.Network
{
    // ------------------------------------------------------------
    // SocketIOResponse-like wrapper (GetValue<T>())
    // ------------------------------------------------------------
    /// <summary>
    /// Wrapper for a raw socket payload that provides typed deserialization helpers.
    /// Mimics a subset of SocketIOResponse semantics for convenience.
    /// </summary>
    public sealed class SocketResponse
    {
        /// <summary>
        /// Raw JSON payload string.
        /// </summary>
        private readonly string _raw;

        /// <summary>
        /// Serializer used to deserialize the payload.
        /// </summary>
        private readonly IJsonSerializer _serializer;

        /// <summary>
        /// Creates a response wrapper for a raw payload string.
        /// </summary>
        /// <param name="raw">Raw JSON payload</param>
        /// <param name="serializer">Serializer for typed deserialization</param>
        public SocketResponse(string raw, IJsonSerializer serializer)
        {
            _raw = raw ?? string.Empty;
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        /// <summary>
        /// Raw payload text as received from the socket.
        /// </summary>
        public string RawText => _raw;

        /// <summary>
        /// Deserialize the payload to the given type.
        /// </summary>
        /// <typeparam name="T">Target type</typeparam>
        /// <returns>Deserialized value</returns>
        public T GetValue<T>() => _serializer.Deserialize<T>(_raw);

        /// <summary>
        /// Parse the payload into a JSON token.
        /// </summary>
        /// <returns>Parsed JSON token or null token if empty</returns>
        public JToken GetToken()
        {
            if (string.IsNullOrWhiteSpace(_raw)) return JValue.CreateNull();
            return JToken.Parse(_raw);
        }
    }
}
