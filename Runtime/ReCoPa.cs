/*
* SPDX-License-Identifier: AGPL-3.0-or-later
* Copyright (C) 2025 Sergej Görzen <sergej.goerzen@gmail.com>
* This file is part of OmiLAXR.
*/
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Newtonsoft.Json.Linq;
using OmiLAXR.Actors.HeartRate;
using OmiLAXR.Components;
using OmiLAXR.Composers;
using OmiLAXR.Context;
using OmiLAXR.Endpoints;
using OmiLAXR.Filters;
using OmiLAXR.Hooks;
using OmiLAXR.Pipelines;
using OmiLAXR.ReCoPa.Endpoints;
using OmiLAXR.ReCoPa.Filters;
using OmiLAXR.ReCoPa.Network;
using OmiLAXR.Types;
using OmiLAXR.xAPI;

#if UNITY_EDITOR
using UnityEditor;
#endif

using UnityEngine;
using UnityEngine.Events;
using UnityEngine.SceneManagement;

namespace OmiLAXR.ReCoPa
{
    /// <summary>
    /// ReCoPa module entry point.
    /// Manages socket connectivity, pipeline hooks, and tracking session lifecycle.
    /// </summary>
    [AddComponentMenu("OmiLAXR / Modules / ReCoPa")]
    [DefaultExecutionOrder(-1)]
    public class ReCoPa : PipelineComponent, IDebugSender
    {
        /// <summary>
        /// Connection URL for the ReCoPa backend (e.g. http://127.0.0.1:4567).
        /// </summary>
        public string connectionUrl = "http://127.0.0.1:4567";

        // ✅ Unity main-thread context
        private SynchronizationContext _unityCtx;

        private void RunOnUnityThread(Action a)
        {
            if (_isShuttingDown) return;
            if (_unityCtx == null) return;
            // Unity's SynchronizationContext executes these on main thread
            _unityCtx.Post(_ => a(), null);
        }

        // TCP Socket client
        private SocketClient _socket;
        private bool _isShuttingDown;
        private EventHandler _onConnectedHandler;
        private EventHandler _onReconnectedHandler;
        private EventHandler _onDisconnectedHandler;
        private EventHandler<int> _onReconnectAttemptHandler;
        private EventHandler<Exception> _onReconnectErrorHandler;
        private EventHandler _onReconnectFailedHandler;
        private EventHandler<string> _onErrorHandler;

        /// <summary>
        /// Target pipeline for tracking and endpoint integration.
        /// </summary>
        [SerializeField] private Pipeline targetPipeline;
        private HeartRateProvider _heartRateProvider;
        private FpsMonitor _fpsMonitor;

        /// <summary>
        /// Active xAPI data provider resolved from the pipeline.
        /// </summary>
        public xApiDataProvider DataProvider { get; private set; }

        private Coroutine _scenarioUpdateCoroutine;

        /// <summary>
        /// Registry for xAPI extensions and activity metadata.
        /// </summary>
        public xApiRegistry xApiRegistry;

        /// <summary>
        /// Endpoints that should receive statements.
        /// </summary>
        [SerializeField] private List<Endpoint> endpoints;

        /// <summary>
        /// Session identifier used for tracking registrations.
        /// </summary>
        private Registration _registration;

        /// <summary>
        /// True if the underlying socket client is connected.
        /// </summary>
        public bool IsConnected => _socket != null && _socket.Connected;

        /// <summary>
        /// UnityEvent fired when the socket connects for the first time.
        /// </summary>
        public UnityEvent onConnected = new UnityEvent();

        /// <summary>
        /// UnityEvent fired when the socket disconnects.
        /// </summary>
        public UnityEvent onDisconnected = new UnityEvent();

        /// <summary>
        /// UnityEvent fired when the socket reconnects.
        /// </summary>
        public UnityEvent onReconnected = new UnityEvent();

        private bool _isTrackingPaused;
        private TrackingScenario? _currentScenario;
        private TrackingConfig? _trackingConfig;
        private readonly List<string> _gameObjects = new();
        private string[] _actions;
        private string[] _gestures;

        private ICalibratable _calibratable;
        private ReCoPaFilter _filter;

        private List<PipelineComponent> _hookedComponents = new List<PipelineComponent>();

        private string sceneName => SceneManager.GetActiveScene().name;
        private bool _isDirty;
        private bool _isMetaDirty;

        /// <summary>
        /// Enables automatic reconnection behaviour.
        /// </summary>
        public bool doReconnection = true;

        /// <summary>
        /// Initial delay for reconnection attempts in milliseconds.
        /// </summary>
        public int reconnectionDelay = 30_000;

        /// <summary>
        /// Maximum delay between reconnection attempts in milliseconds.
        /// </summary>
        public int reconnectionMaxDelay = 60_000;

        /// <summary>
        /// Maximum number of reconnection attempts.
        /// </summary>
        public int reconnectionAttempts = 10;

        private TComponent HookInto<TComponent, TPipeline, TDataProvider>() 
            where TComponent : PipelineComponent
            where TPipeline : LearnerPipeline 
            where TDataProvider : xApiDataProvider
        {
            // find pipeline and data provider
#if UNITY_2021_1_OR_NEWER
            var pipeline = FindAnyObjectByType<TPipeline>();
            var dataProvider = FindAnyObjectByType<TDataProvider>();
#else
            var pipeline = FindObjectOfType<TPipeline>();
            var dataProvider = FindObjectOfType<TDataProvider>();
#endif
            
            // get target component
            var component = gameObject.GetComponentInChildren<TComponent>();
            
            // store target component
            _hookedComponents.Add(component);
            
            // add to OmiLAXR pipeline
            if (component is Endpoint endpoint)
                dataProvider.Endpoints.Add(endpoint);
            else if (component is Hook hook)
                dataProvider.Hooks.Add(hook);
            else if (component is IComposer composer)
                dataProvider.Composers.Add(composer);
            else 
                pipeline.Add(component);
            
            return component;
        }
        
        /// <summary>
        /// Builds a <see cref="TrackingMeta"/> snapshot for transmission.
        /// </summary>
        /// <param name="metaContext">Additional context string to include</param>
        /// <returns>Populated tracking metadata</returns>
        public TrackingMeta GetMeta(string metaContext) => new TrackingMeta()
        {
            //isTracking = targetPipeline.IsRunning,
            isTrackingPaused = _isTrackingPaused,
            isCalibrated = _calibratable?.IsCalibrated ?? false,
            isCalibratable = _calibratable != null,
            computerName = Environment.MachineName,
            actorName = targetPipeline.actor.actorName,
            actorEmail = targetPipeline.actor.actorEmail,
            activeActorName = targetPipeline.actor.actorName,
            activeActorEmail = targetPipeline.actor.actorEmail,
            sessionId = _registration.uuid,
            endpoints = GetEndpointNames(),
            filters = GetFilterNames(),
            actions = _actions ?? Array.Empty<string>(),
            gestures = _gestures ?? Array.Empty<string>(),
            heartRate = _heartRateProvider?.GetHeartRate(),
            fps = _fpsMonitor?.CurrentFPS,
            metaContext = metaContext,
        };

        private void Awake()
        {
            // Keep socket loop/coroutines alive when Unity loses focus (Editor/background).
            Application.runInBackground = true;

            // ✅ capture Unity main thread context
            _unityCtx = SynchronizationContext.Current;
            if (_unityCtx == null)
            {
                // Very rare in Unity, but fail loudly so you notice
                Debug.Error("[ReCoPa] SynchronizationContext is null. Main-thread dispatch will not work.");
                _unityCtx = new SynchronizationContext();
                SynchronizationContext.SetSynchronizationContext(_unityCtx);
            }

#if UNITY_2021_1_OR_NEWER
            targetPipeline = FindFirstObjectByType<LearnerPipeline>();
            xApiRegistry = FindFirstObjectByType<xApiRegistry>();
            _registration = FindFirstObjectByType<Registration>();
#else
            targetPipeline = FindObjectOfType<LearnerPipeline>();
            xApiRegistry = FindObjectOfType<xApiRegistry>();
            _registration = FindObjectOfType<Registration>();
#endif
            _fpsMonitor = targetPipeline.GetComponentInParent<FpsMonitor>();
            _heartRateProvider = targetPipeline.GetComponentInParent<HeartRateProvider>();
            DataProvider = targetPipeline.GetDataProvider<xApiDataProvider>();
            
            _filter = HookInto<ReCoPaFilter, LearnerPipeline, xApiDataProvider>();
            
            var endpoint = HookInto<ReCoPaEndpoint, LearnerPipeline, xApiDataProvider>();
            endpoint.OnSentStatement += SendStatement;

            _calibratable = targetPipeline.GetComponentInChildren<ICalibratable>();

            targetPipeline.enabled = false;
            Init();
            InitSocket();
            targetPipeline.enabled = true;
        }

        private void OnDisable()
        {
            CleanupSocket();
        }

        private void Update()
        {
            var dt = Time.unscaledDeltaTime;
            if (dt > 0f)
            {
            }
        }

        private void OnDestroy()
        {
            CleanupSocket();
        }
        
        private void Init()
        {
            if (_calibratable != null)
            {
                _calibratable.OnCalibrationStarted += () => SendMeta("calibration:start");
                _calibratable.OnCalibrationEnded += _ => SendMeta("calibration:stop");
            }

            if (!targetPipeline)
            {
                DebugLog.Warning("Cannot find a <LearnerPipeline>.");
                return;
            }

            targetPipeline.AfterFoundObjects += objects =>
            {
                if (objects == null) return;
                _gameObjects.AddRange(objects.Select(o => o.GetTrackingName()));
                _gameObjects.Sort();
            };
            targetPipeline.AfterStartedPipeline += HookIntoLearner;
        }

        private void SendStatement(Endpoint _, IStatement statement)
        {
            if (_socket == null) return;
            _socket.Emit("statement", statement.ToJsonString());
        }

        private void HookIntoLearner(Pipeline p)
        {
            _actions = p.Actions.Keys.ToArray();
            Array.Sort(_actions);

            _gestures = p.Gestures.Keys.ToArray();
            Array.Sort(_gestures);

            var config = new TrackingConfig()
            {
                gameObjects = _gameObjects.ToArray(),
                gestures = _gestures,
                actions = _actions
            };
            
            // enable all hooked items
            foreach (var hooked in _hookedComponents) 
                hooked.enabled = true;
            
            p.AfterStartedPipeline += (_) =>
            {
                SendMeta("tracking:start");
            };
            
            p.BeforeStoppedPipeline += (_) =>
            {
                SendMeta("tracking:stop");
            };
            
            _ = _socket.EmitAsync("tracking", JObject.FromObject(config).ToString());
        }

        private void StartTracking()
        {
            foreach (var e in endpoints) e.StartSending();
            targetPipeline.StartPipeline();
        }

        private void PauseTracking()
        {
            foreach (var e in endpoints) e.PauseSending();
            _isTrackingPaused = true;
        }

        private void ResumeTracking()
        {
            foreach (var e in endpoints) e.StartSending();
            _isTrackingPaused = false;
        }

        private void StopTracking()
        {
            foreach (var e in endpoints) e.StopSending();
            targetPipeline.StopPipeline();
        }

        private void InitSocket()
        {
            if (_socket != null) return;

            _socket = new SocketClient(connectionUrl, new SocketClientOptions()
            {
                Reconnection = doReconnection,
                ReconnectionDelay = reconnectionDelay,
                ReconnectionDelayMax = reconnectionMaxDelay,
                ReconnectionAttempts = reconnectionAttempts,
                SessionId = _registration != null ? _registration.uuid : string.Empty,
                ExtraHeaders = new Dictionary<string, string>()
                {
                    ["clientType"] = "participant",
                    ["version"] = "2.0.0"
                }
            });

            _onConnectedHandler ??= (_, _) =>
            {
                if (_isShuttingDown || !this) return;
                OnConnected();
            };
            _onReconnectedHandler ??= (_, _) =>
            {
                if (_isShuttingDown || !this) return;
                OnReconnected();
            };
            _onDisconnectedHandler ??= (_, _) =>
            {
                if (_isShuttingDown || !this) return;
                OnDisconnected();
            };
            _onReconnectAttemptHandler ??= (_, i) =>
            {
                if (_isShuttingDown || !this) return;
                DebugLog.Warning("Reconnecting to ReCoPa... Attempt " + i);
            };
            _onReconnectErrorHandler ??= (_, ex) =>
            {
                if (_isShuttingDown || !this) return;
                DebugLog.Error($"Reconnection error '{ex}'.");
            };
            _onReconnectFailedHandler ??= (_, _) =>
            {
                if (_isShuttingDown || !this) return;
                DebugLog.Error("Failed connecting to ReCoPa. Make sure you have started it.");
                enabled = false;
            };
            _onErrorHandler ??= (_, msg) =>
            {
                if (_isShuttingDown || !this) return;
                DebugLog.Error($"Error '{msg}'.");
            };

            _socket.OnConnected += _onConnectedHandler;
            _socket.OnReconnected += _onReconnectedHandler;
            _socket.OnDisconnected += _onDisconnectedHandler;

            _socket.OnReconnectAttempt += _onReconnectAttemptHandler;
            _socket.OnReconnectError += _onReconnectErrorHandler;
            _socket.OnReconnectFailed += _onReconnectFailedHandler;

            _socket.OnError += _onErrorHandler;

            _socket.On("quit", _ =>
            {
                if (_isShuttingDown || !this) return;
                RunOnUnityThread(Quit);
            });
            _socket.On("all", _ =>
            {
                if (_isShuttingDown || !this) return;
                _isDirty = true;
                _isMetaDirty = true;
            });

            _socket.On("scenario", payload =>
            {
                if (_isShuttingDown || !this) return;
                DispatchScenarioInformation(payload);
            });

            _socket.On("calibration:start", _ =>
            {
                if (_isShuttingDown || !this || _calibratable == null) return;
                _calibratable.StartCalibration();
            });
            _socket.On("calibration:stop", _ =>
            {
                if (_isShuttingDown || !this || _calibratable == null) return;
                _calibratable.StopCalibration();
            });

            _socket.On("tracking", payload =>
            {
                if (_isShuttingDown || !this) return;
                DispatchTrackingInformation(payload);
            });
            _socket.On("tracking:start", payload =>
            {
                if (_isShuttingDown || !this) return;
                DispatchStartTracking(payload);
            });
            _socket.On("tracking:stop", payload =>
            {
                if (_isShuttingDown || !this) return;
                DispatchStopTracking(payload);
            });
            _socket.On("tracking:pause", payload =>
            {
                if (_isShuttingDown || !this) return;
                DispatchPauseTracking(payload);
            });
            _socket.On("tracking:resume", payload =>
            {
                if (_isShuttingDown || !this) return;
                DispatchResumeTracking(payload);
            });

            _ = _socket.ConnectAsync();
        }

        private void OnConnected()
        {
            // Send meta information first time
            DebugLog.Print("Connected to ReCoPa.");
            onConnected.Invoke();
            SendMeta("connected");
            BeginScenarioUpdate();
        }

        private void OnReconnected()
        {
            DebugLog.Print("Reconnected to ReCoPa.");
            onReconnected.Invoke();
            SendMeta("reconnected");
            BeginScenarioUpdate();
        }

        private void BeginScenarioUpdate()
        {
            _isDirty = true;
            _scenarioUpdateCoroutine = StartCoroutine(UpdateScenario());
        }

        private void OnDisconnected()
        {
            if (_isShuttingDown || !this) return;
            DebugLog.Print("Disconnected from ReCoPa.");
            onDisconnected.Invoke();

            if (_scenarioUpdateCoroutine != null)
                StopCoroutine(_scenarioUpdateCoroutine);
        }

        private static void Quit()
        {
#if UNITY_EDITOR
            EditorApplication.isPlaying = false;
#else
            Application.Quit();
#endif
        }

        private void OnApplicationQuit()
        {
            CleanupSocket();
        }

        private void CleanupSocket()
        {
            if (_isShuttingDown) return;
            _isShuttingDown = true;

            if (_scenarioUpdateCoroutine != null)
            {
                StopCoroutine(_scenarioUpdateCoroutine);
                _scenarioUpdateCoroutine = null;
            }

            if (_socket == null) return;

            if (_onConnectedHandler != null) _socket.OnConnected -= _onConnectedHandler;
            if (_onReconnectedHandler != null) _socket.OnReconnected -= _onReconnectedHandler;
            if (_onDisconnectedHandler != null) _socket.OnDisconnected -= _onDisconnectedHandler;
            if (_onReconnectAttemptHandler != null) _socket.OnReconnectAttempt -= _onReconnectAttemptHandler;
            if (_onReconnectErrorHandler != null) _socket.OnReconnectError -= _onReconnectErrorHandler;
            if (_onReconnectFailedHandler != null) _socket.OnReconnectFailed -= _onReconnectFailedHandler;
            if (_onErrorHandler != null) _socket.OnError -= _onErrorHandler;

            _socket.Disconnect();
            _socket.Dispose();
            _socket = null;
        }

        /// <summary>
        /// Sends meta information to socket server.
        /// </summary>
        private void SendMeta(string metaContext)
        {
            if (_socket == null || _isShuttingDown) return;

            // already on unity thread typically, but safe:
            var socket = _socket;
            RunOnUnityThread(() =>
            {
                if (_isShuttingDown || socket == null) return;
                _ = socket.EmitAsync("info", GetMeta(metaContext));
            });
            _isMetaDirty = false;
        }

        private IEnumerator UpdateScenario()
        {
            while (true)
            {
                if (_isShuttingDown || _socket == null)
                    yield break;

                if (_socket.Connected && _isDirty)
                    SendScenario();

                if (_socket.Connected)
                    SendMeta(_isMetaDirty ? "refresh" : "update");

                yield return new WaitForSeconds(1);
            }
        }

        /// <summary>
        /// Sends all gameObjects and actions to socket server.
        /// </summary>
        private void SendScenario(bool reload = false)
        {
            if (_socket == null) return;

            var scenario = GetScenario(reload);
            var tracking = GetTrackingConfig(scenario);

            _ = _socket.EmitAsync("scenario", scenario);
            _ = _socket.EmitAsync("tracking", tracking);

            DebugLog.Print("Sent scenario information.");
            _isDirty = false;
        }
        /// <summary>
        /// Returns the tracking configuration for the current scenario.
        /// </summary>
        /// <returns>Tracking configuration derived from the active scenario</returns>
        public TrackingConfig GetScenarioTrackingConfig() => GetTrackingConfig(GetScenario());

        /// <summary>
        /// Builds or returns a cached tracking configuration for the given scenario.
        /// </summary>
        /// <param name="scenario">Scenario used to populate tracking config</param>
        /// <returns>Tracking configuration</returns>
        public TrackingConfig GetTrackingConfig(TrackingScenario scenario)
        {
            if (_trackingConfig.HasValue) 
                return _trackingConfig.Value;

            var lrs = FindAnyObjectByType<OmiLAXR.xAPI.Endpoints.LearningRecordStore>();
            var actor = targetPipeline.actor;
            
            if (xApiRegistry == null)
                xApiRegistry = FindFirstObjectByType<xApiRegistry>();
            
            var uri = xApiRegistry.uri;

            var credentials = lrs.Credentials;
            var endpoint = credentials.endpoint;
            var key = credentials.username;
            var secret = credentials.password;
            var actorName = actor.actorName;
            var actorEmail = actor.actorEmail;
            
            if(!actorEmail.StartsWith("mailto:"))
                actorEmail = "mailto:" + actorEmail;
            
            _trackingConfig = new TrackingConfig()
            {
                auth = new TrackingConfig.ClientAuth(key, secret),
                lrs = endpoint,
                uri = uri,
                identity = new TrackingConfig.TrackingIdentity(actorName, actorEmail),
                gameObjects = scenario.gameObjects,
                actions = scenario.actions,
                gestures = scenario.gestures
            };

            return _trackingConfig.Value;
        }
        
        private void SetupEndpoints(EndpointConfigs map)
        {
            // setup endpoint configs
            var endpoints = FindObjectsByType<Endpoint>(FindObjectsInactive.Exclude, FindObjectsSortMode.None);
            foreach (var endpoint in endpoints)
            {
                var epName = endpoint.GetType().Name;
                var config = map[epName];
                if (config == null)
                    continue;
                endpoint.ConsumeDataMap(config);
            }
        }

        private void DispatchStartTracking(SocketResponse e)
        {
            var config = e.GetValue<TrackingConfig>();
            foreach (var endpoint in endpoints)
            {
                //endpoint.ConsumeDataMap(config);
            }

            if (_trackingConfig.HasValue)
                SetupEndpoints(_trackingConfig.Value.endpoints);

            if (xApiRegistry == null)
                xApiRegistry = FindFirstObjectByType<xApiRegistry>();
                
            xApiRegistry.uri = config.uri;

            // apply game objects filter
            _filter.gameObjects = config.gameObjects;
                
            // disable all actions
            targetPipeline.SetDisabledActions(true);
            // enable only selected actions
            targetPipeline.SetDisabledActions(false, config.actions);
                
            // disable all gestures
            targetPipeline.SetDisabledGestures(true);
            // enable only selected gestures
            targetPipeline.SetDisabledGestures(false, config.gestures);

            StartTracking();
        }

        private void DispatchPauseTracking(SocketResponse _) => PauseTracking();
        private void DispatchResumeTracking(SocketResponse _) => ResumeTracking();
        private void DispatchStopTracking(SocketResponse _) => StopTracking();

        private void DispatchTrackingInformation(SocketResponse _)
        {
            var tracking = GetScenarioTrackingConfig();
            _socket.EmitAsync("tracking", JObject.FromObject(tracking));
        }

        private void DispatchScenarioInformation(SocketResponse _)
        {
            var scenario = GetScenario();
            _socket.EmitAsync("scenario", JObject.FromObject(scenario));
        }
        
        /// <summary>
        /// Returns the current tracking scenario, optionally forcing a rebuild.
        /// </summary>
        /// <param name="reload">If true, rebuilds the scenario from current state</param>
        /// <returns>Tracking scenario snapshot</returns>
        public TrackingScenario GetScenario(bool reload = false)
        {
            if (!reload && _currentScenario.HasValue) return _currentScenario.Value;

            _currentScenario = new TrackingScenario()
            {
                name = sceneName,
                gameObjects = _gameObjects.ToArray(),
                actions = _actions,
                gestures = _gestures
            };
            return _currentScenario.Value;
        }

        private string[] GetEndpointNames()
        {
            var found = FindObjectsByType<Endpoint>(FindObjectsInactive.Exclude, FindObjectsSortMode.None);
            var names = found.Select(endpoint => endpoint.GetType().Name)
                .Distinct()
                .OrderBy(name => name)
                .ToArray();
            return names.Length > 0 ? names : Array.Empty<string>();
        }

        private string[] GetFilterNames()
        {
            var found = FindObjectsByType<Filter>(FindObjectsInactive.Exclude, FindObjectsSortMode.None);
            var names = found.Select(filter => filter.GetType().Name)
                .Distinct()
                .OrderBy(name => name)
                .ToArray();
            return names.Length > 0 ? names : Array.Empty<string>();
        }


        private static bool TryReadNumericMember(Func<object> read, out float value)
        {
            value = 0f;
            try
            {
                var raw = read();
                if (raw == null) return false;

                if (raw is float f)
                {
                    value = f;
                    return value > 0f;
                }
                if (raw is double d)
                {
                    value = (float)d;
                    return value > 0f;
                }
                if (raw is int i)
                {
                    value = i;
                    return value > 0f;
                }
                if (raw is long l)
                {
                    value = l;
                    return value > 0f;
                }
                if (raw is short s)
                {
                    value = s;
                    return value > 0f;
                }
                if (raw is byte b)
                {
                    value = b;
                    return value > 0f;
                }
            }
            catch
            {
                return false;
            }

            return false;
        }

        private static readonly DebugLog Debug = new DebugLog("ReCoPa Module");

        /// <summary>
        /// Logger instance for ReCoPa module diagnostics.
        /// </summary>
        public DebugLog DebugLog => Debug;
    }
}
