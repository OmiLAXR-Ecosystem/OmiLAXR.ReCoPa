# AGENTS.md

How to work in `com.rwth.unity.omilaxr.recopa`. Architecture and commands are in `CLAUDE.md`. Workspace-wide context is in `../CLAUDE.md`.

## Repository and releases

- This folder is its own git repo inside the `omilaxr-ecosystem` workspace. Run git commands here.
- Work for a release goes on a branch `vX.Y.Z`, which is merged into `main` when done.
- Commit messages: `type(scope): summary`, where type is `feat`, `fix`, `refactor`, `test` or `docs`. Do not commit unrelated working-tree changes that you did not make.
- There is no publish workflow in this repo. Releases are done manually.

## CHANGELOG

- Every user-visible change gets a line in `CHANGELOG.md` in the same change or commit. Do not leave it for later.
- Newest version on top. Heading format in this file: `## [X.Y.Z] - YYYY-MM-DD`. Keep the file's existing style.
- Add entries under the topmost version if it is not released yet (no `vX.Y.Z` tag). Otherwise start a new version and do the version bump below.
- Group entries under `### Added`, `### Changed`, `### Fixed` (and `### Removed`/`### Deprecated` when needed). Write one line per change, in English, and name types and members in backticks.
- Mark breaking changes explicitly with **Breaking:** and say what users must change.
- Changes that only touch tests or CI need no entry. Refactors need one if they add public API, such as new interfaces or overloads.

## Version bump

- SemVer: patch for fixes, minor for new backward-compatible API, major for breaking changes.
- Update every place that holds the version, in one commit:
  - `package.json`
  - `CITATION.cff` (`version`, `date-released`)
  - `CHANGELOG.md`
  - `README.md`: the `"com.rwth.unity.omilaxr.recopa": "X.Y.Z"` install snippet (and the core version under *Dependencies* when the pin changes)
- The release date in `CITATION.cff` and the CHANGELOG heading is the day of the release. Update it when tagging.

## Core dependency

- `package.json` pins the core (`com.rwth.unity.omilaxr`). Do not raise it on routine version bumps.
- Raise it when this package starts using core API that is newer than the pin, or when the core had an important or breaking change. Also update README lines that mention the core version.

## Unity package rules

- Every new file and folder needs a committed `.meta`. Let Unity create it (open `OmiLAXR_Dev`), or copy a neighbor's `.meta` with a fresh random 32-hex `guid`. Never delete or regenerate existing `.meta` files: prefabs, scenes and asmdefs reference their GUIDs. Folders ending in `~` (`Samples~`) have no `.meta`.
- The minimum Unity version is `2021.3` (`package.json` `unity`), but development happens on Unity 6. Put version-specific APIs behind `#if UNITY_2023_1_OR_NEWER` (or similar).
- Serialized field names are part of the scene and prefab format. Rename them only with `[FormerlySerializedAs]`.
- New components get `[AddComponentMenu("OmiLAXR / …")]`. Copy the path from a component of the same kind.
- License: AGPL-3.0-or-later. Source files start with the SPDX header (copy from a neighbor).
- Add or adjust tests in `Tests/` with every behaviour change. Name test files `<Subject>_Tests.cs`, name methods `Subject_Does_Something`, and use `// Arrange / Act / Assert` comments.

## ReCoPa-specific rules

- The socket protocol is shared with the ReCoPa desktop app in the workspace `ReCoPa/`. Any change to event names, payload DTOs or framing must be made on both sides. Point this out to the user, because they are separate repos and releases.
- The asmdef references `OmiLAXR.xAPI` and `xAPI.Registry`, but `package.json` only declares the core. Keep that in mind when you touch dependencies.
