/*
* SPDX-License-Identifier: AGPL-3.0-or-later
* Copyright (C) 2025 Sergej Görzen <sergej.goerzen@gmail.com>
* This file is part of OmiLAXR.
*/
using System;
using OmiLAXR.Utils;

namespace OmiLAXR.ReCoPa
{
    /// <summary>
    /// Configuration payload for ReCoPa tracking.
    /// Contains LRS connection data, identity, and filtering settings.
    /// </summary>
    public struct TrackingConfig
    {
        /// <summary>
        /// Client authentication credentials for LRS or backend access.
        /// </summary>
        public struct ClientAuth
        {
            /// <summary>
            /// Client key or username.
            /// </summary>
            public string key;

            /// <summary>
            /// Client secret or password.
            /// </summary>
            public string secret;

            /// <summary>
            /// Creates a new client auth configuration.
            /// </summary>
            /// <param name="key">Client key</param>
            /// <param name="secret">Client secret</param>
            public ClientAuth(string key, string secret)
            {
                this.key = key;
                this.secret = secret;
            }

            /// <summary>
            /// Returns a readable string representation for debugging.
            /// </summary>
            /// <returns>Formatted auth description</returns>
            public override string ToString()
            {
                // Never print the secret, configs are logged
                return $"[ClientAuth: key={key}, secret={(string.IsNullOrEmpty(secret) ? "" : "***")}]";
            }
        }
        
        /// <summary>
        /// Identity information for the tracked actor.
        /// </summary>
        public struct TrackingIdentity
        {
            /// <summary>
            /// Actor email address.
            /// </summary>
            public string email;

            /// <summary>
            /// Actor display name.
            /// </summary>
            public string name;
        
            /// <summary>
            /// Creates a new tracking identity.
            /// </summary>
            /// <param name="name">Actor name</param>
            /// <param name="email">Actor email</param>
            public TrackingIdentity(string name, string email)
            {
                this.name = name;
                this.email = email;
            }
        
            /// <summary>
            /// Returns a readable string representation for debugging.
            /// </summary>
            /// <returns>Formatted identity description</returns>
            public override string ToString()
            {
                return $"[TrackingIdentity email={email}, name={name}]";
            }
        }

        /// <summary>
        /// LRS endpoint URL.
        /// </summary>
        public string lrs;

        /// <summary>
        /// Base URI for the tracking backend.
        /// </summary>
        public string uri;

        /// <summary>
        /// Client authentication credentials.
        /// </summary>
        public ClientAuth auth;

        /// <summary>
        /// Actor identity for the tracking session.
        /// </summary>
        public TrackingIdentity identity;

        /// <summary>
        /// GameObject tracking names to include or exclude (based on <see cref="isBlacklist"/>).
        /// </summary>
        public string[] gameObjects;

        /// <summary>
        /// Action identifiers to include or exclude.
        /// </summary>
        public string[] actions;

        /// <summary>
        /// Gesture identifiers to include or exclude.
        /// </summary>
        public string[] gestures;

        /// <summary>
        /// If true, listed items are excluded (blacklist); otherwise they are included (whitelist).
        /// </summary>
        public bool isBlacklist;

        /// <summary>
        /// Endpoint configurations keyed by endpoint id.
        /// </summary>
        public EndpointConfigs endpoints;

        /// <summary>
        /// Returns a readable string representation for debugging.
        /// </summary>
        /// <returns>Formatted config description</returns>
        public override string ToString()
        {
            return $"[TrackingConfig: lrs={lrs}, uri={uri}, auth={auth}, identity={identity}, gameObjects={Array(gameObjects)}, actions={Array(actions)}, gestures={Array(gestures)}, isBlackList={isBlacklist}]";
        }

        /// <summary>
        /// Formats a string array for debug output.
        /// </summary>
        /// <param name="array">Array to format</param>
        /// <returns>Formatted array string</returns>
        private static string Array(string[] array)
        {
            var str = array != null ? string.Join(",", array) : null;
            return $"[Array: [{str}]]";
        }
    }
}
