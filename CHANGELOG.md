# Changelog

## [0.0.2] - 2026-11-09

### Added
- `IFrameCodec` (`LengthPrefixedFrameCodec`), `IReconnectPolicy` (`ExponentialBackoffReconnectPolicy` with an injectable jitter source) and `ISocketTransport` (`TcpSocketTransport`) in `OmiLAXR.ReCoPa.Network`.
- `SocketClient` constructor overload that takes a frame codec, reconnect policy and transport. The existing constructor is unchanged.

### Changed
- Updated package version metadata.

### Fixed
- `localhost:4567` and other `host:port` values without a scheme are no longer parsed as a URI without host.
- Receive and send timeouts reconnect the client instead of ending its loop.
- A failed heartbeat closes the socket, so the client reconnects.
- Concurrent frame writes from the heartbeat and the caller are serialized.
- When a send fails while flushing the queue, the failed message and all remaining ones go back into the queue in their original order.
- Reading no longer hangs on runtimes where `NetworkStream.ReadAsync` ignores the cancellation token.
- `ClientAuth.ToString()` no longer prints the secret.
- The hello frame no longer throws an `ArgumentNullException` when `SocketClientOptions.SessionId` is set and `ExtraHeaders` is `null`. It sends empty headers instead.
- `SocketResponse` uses `Newtonsoft.Json.Linq` instead of `Unity.Plastic.Newtonsoft.Json.Linq`. The Plastic copy belongs to the editor-only Version Control package, so player builds and projects without that package failed to compile.

## [0.0.1] - 2026-02-05

### Added
- First public release of OmiLAXR.ReCoPa.
- Establishes a connection to eCoPa V2 tool.

### Changed
- Initial packaging and metadata for Unity Package Manager.

### Fixed
- N/A
