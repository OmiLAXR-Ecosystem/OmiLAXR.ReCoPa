# OmiLAXR.ReCoPa

[![DOI](https://zenodo.org/badge/1118470726.svg)](https://doi.org/10.5281/zenodo.18496701)

Unity package that connects OmiLAXR to ReCoPa (Researcher Companion Panel). It synchronizes scenario and tracking metadata, exchanges configuration details, and relays OmiLAXR xAPI statements while enabling remote session control hooks.

**Highlights**
- Socket connection to ReCoPa with automatic reconnection.
- Scenario + tracking metadata synchronization.
- Forwarding of xAPI statements through the ReCoPa endpoint.
- Remote control hooks for pause/resume and calibration events.
- Optional filters for limiting tracked game objects.

## Compatibility

This package targets Unity `2021.3` and is tested with `2021.3.15f1` (see `package.json`).

## Dependencies

- `com.rwth.unity.omilaxr` `2.1.1`

## Install

### Install Using Git URL

1. Go to **Window** -> **Package Manager**.
2. Click the `+` button.
3. Select **Add package from git URL**.
4. Paste `https://github.com/SGoerzen/OmiLAXR.ReCoPa.git` and confirm.

### Install via `manifest.json`

Add this to `Packages/manifest.json`:

```json
{
  "dependencies": {
    "com.rwth.unity.omilaxr.recopa": "0.0.2"
  }
}
```

## Quick Start

1. Make sure your scene includes a `LearnerPipeline` from OmiLAXR.
2. Add the `ReCoPa` component via **Add Component** -> `OmiLAXR / Modules / ReCoPa`.
3. Set `connectionUrl` to your ReCoPa server (default: `http://127.0.0.1:4567`).
4. Enter Play mode. The module will connect, publish scenario/tracking metadata, and keep it synchronized.

## How It Works

- The `ReCoPa` component hooks a `ReCoPaFilter` and `ReCoPaEndpoint` into the OmiLAXR pipeline at runtime.
- Scenario data (scene name, tracked objects, actions, gestures) is sent to ReCoPa.
- Tracking configuration (LRS endpoint + credentials, actor identity, selected actions/gestures) is shared with ReCoPa.
- Statements produced by OmiLAXR are forwarded via the ReCoPa endpoint.
- ReCoPa can trigger calibration events and request pause/resume.

## Configuration

Key settings on the `ReCoPa` component:

- `connectionUrl`: ReCoPa server URL.
- `doReconnection`: enable/disable automatic reconnection.
- `reconnectionDelay`, `reconnectionMaxDelay`, `reconnectionAttempts`: reconnection behavior.

## License

AGPL-3.0-or-later. See `LICENSE`.

## Changelog

See `CHANGELOG.md`.
