# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Working rules (versioning, CHANGELOG, `.meta` files, licenses, compatibility) are in @AGENTS.md. Follow them.

## What this is

`com.rwth.unity.omilaxr.recopa`: Bridge module between OmiLAXR and the ReCoPa (Researcher Companion Panel) desktop app. It syncs scenario and tracking metadata, forwards statements live, and accepts remote control commands (pause/resume, calibration).

## Commands

There is no command-line build or linter: the package compiles inside a Unity project. Paths below are relative to the workspace root (`..`).

- **Develop:** `OmiLAXR_Dev/` (Unity 6000.3.13f1) does not link this package. Add `"com.rwth.unity.omilaxr.recopa": "file:<abs path to OmiLAXR.ReCoPa>"` to `OmiLAXR_Dev/Packages/manifest.json`, together with any packages its asmdef references.
- **Tests** (`Tests/`, assembly `OmiLAXR.ReCoPa.Tests.Runtime`): the test asmdef is a runtime assembly, so the tests are PlayMode-only. Run them in the Unity Test Runner (PlayMode tab).
  The tests only show up if `com.rwth.unity.omilaxr.recopa` is listed under `"testables"` in `OmiLAXR_Dev/Packages/manifest.json`. It currently is not.
- **CLI test run.** Batch mode fails while the editor has the project open. In that case, run against an APFS copy (`cp -c -R OmiLAXR_Dev <scratch>/Dev`).

  ```bash
  /Applications/Unity/Hub/Editor/6000.3.13f1/Unity.app/Contents/MacOS/Unity -batchmode \
    -projectPath OmiLAXR_Dev -runTests -testPlatform PlayMode \
    -assemblyNames OmiLAXR.ReCoPa.Tests.Runtime -testResults results.xml
  # single test: replace -assemblyNames … with -testFilter OmiLAXR.ReCoPa.Tests.<Class>.<Method>
  ```

## Architecture

- **`ReCoPa`** (`Runtime/ReCoPa.cs`, menu `OmiLAXR / Modules / ReCoPa`) is the orchestrator.
  - It connects to `connectionUrl` (default `http://127.0.0.1:4567`).
  - It emits `info`, `scenario`, `tracking` and `statement`.
  - It handles `quit`, `all`, `scenario`, `tracking`, `tracking:start/stop/pause/resume` and `calibration:start/stop`.
  - It resolves the `xApiDataProvider` and `LearningRecordStore` and shares the LRS config with the desktop app.
- **Wiring**: `LearnerPipelineExtension` adds `ReCoPaFilter` (limits tracked objects to the researcher's selection) and `ReCoPaEndpoint` (forwards statements over the socket) to the learner pipeline. `ReCoPaTrackingBehaviour` and the `Researcher` actor are in `Runtime/TrackingBehaviours` and `Runtime/Actors`.
- **DTOs**: `TrackingScenario`, `TrackingConfig`, `TrackingMeta`, `EndpointConfig` and `ReconnectConfiguration`, serialized with Newtonsoft.
- **Network** (`Runtime/Network`): `SocketClient` handles event handlers, reconnects and a pluggable `IJsonSerializer` (default Newtonsoft).
  - Its collaborators are interfaces in `SocketComponents.cs`: `IFrameCodec` (`LengthPrefixedFrameCodec`: `[int32 bodyLen BE][uint16 eventLen BE][event UTF-8][payload UTF-8]`), `IReconnectPolicy` (`ExponentialBackoffReconnectPolicy`, ±10 % jitter) and `ISocketTransport` (`TcpSocketTransport`).
  - Options are in `SocketClientOptions`. Tests inject fakes through the `SocketClient(url, options, transport, codec, policy)` overload instead of opening TCP.
