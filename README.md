# MQTT Explorer Bridge

An Indigo plugin that captures MQTT traffic from one or more brokers and serves
it to a browser-based MQTT Explorer page — a live, hierarchical, retained-aware
view of every topic, with publish support.

A replacement for the increasingly-stale MQTT Explorer desktop app, running as
a first-class Indigo plugin.

## Features

- One Indigo device per broker — connect to multiple brokers concurrently
- Subscribes to `#` by default (configurable per device, comma-separated)
- Maintains an in-memory tree of every topic with per-topic message history
- Embedded WebSocket server pushes live messages to the browser
- Self-contained HTML page: collapsible tree, JSON pretty-print, retained
  badges, history, publish panel (topic / payload / QoS / retain)
- Coalesces rapid updates per topic to keep the UI responsive under firehose load
- Auth via shared token (from `IndigoSecrets.py` or PluginConfig)
- Publish capability gated by a plugin-level toggle (read-only mode)

## Architecture

```
Browser ──HTTPS──▶ Indigo IWS (8176)  serves the static HTML page
   │
   └─WS to plugin :9876 ────────▶ MQTTExplorerBridge plugin
                                      │
                                      ├── paho-mqtt clients (one per broker)
                                      └── In-memory tree + per-topic history
```

Indigo's own WebSocket isn't used for the MQTT firehose — broadcasting every
message via device state updates would flood every Indigo client on the network.
Instead the plugin runs its own small WS server.

## Installation

1. Open the Releases page on GitHub and download `MQTTExplorerBridge.indigoPlugin.zip`
2. Unzip — you will get `MQTTExplorerBridge.indigoPlugin`
3. Double-click the bundle — Indigo will install it
4. Enable the plugin and configure (see below)

## Credentials

The plugin reads MQTT credentials from `IndigoSecrets.py` first, then falls
back to per-device fields. That file lives at
`/Library/Application Support/Perceptive Automation/IndigoSecrets.py` — if you do
not have it yet, copy `IndigoSecrets_example.py` (shipped with the CliveS plugins)
into that folder and rename the copy to `IndigoSecrets.py`. Add the following to it:

```python
MQTT_BROKER         = "192.168.1.20"
MQTT_PORT           = 1883
MQTT_USERNAME       = ""
MQTT_PASSWORD       = ""
MQTT_EXPLORER_TOKEN = "pick-something-long-and-random"
```

`MQTT_EXPLORER_TOKEN` is the auth token required on the WebSocket — the HTML
page passes it as `?token=…`. Anyone with the token can read all MQTT traffic
and publish (if publishing is enabled), so treat it like a password.

## Configuration

### Plugin config (Plugins → MQTT Explorer Bridge → Configure)

| Field | Default | Notes |
|---|---|---|
| WebSocket Port | 9876 | Plugin's WS server port |
| WS Bind Address | 0.0.0.0 | `127.0.0.1` for local-only, `0.0.0.0` for LAN |
| WS Auth Token | — | Blank uses `MQTT_EXPLORER_TOKEN` from `IndigoSecrets.py` |
| Allow Publish from Web UI | on | Off = read-only explorer |
| WS Coalesce Window (ms) | 100 | Per-topic update coalescing |
| Debug Logging | off | |

### Broker device

Create a device of type **MQTT Broker** per broker. Leaving `Broker Host` blank
uses `MQTT_BROKER` from `IndigoSecrets.py`.

| Field | Default | Notes |
|---|---|---|
| Broker Host | 192.168.1.20 | |
| Broker Port | 1883 | |
| Username / Password | — | Blank uses MQTT_USERNAME/PASSWORD from secrets |
| Use TLS | off | |
| Client ID | indigo-mqtt-explorer | Device ID is appended automatically |
| History per topic | 50 | Older messages drop off the deque |
| Subscribe Topic | `#` | Comma-separate for multiple filters |

## Using the explorer page

After installing the plugin and creating at least one broker device, open the
page in your browser. Indigo's IWS serves **HTTP on port 8176** by default —
using HTTP avoids `ws://` mixed-content blocking:

```
http://192.168.1.20:8176/com.clives.indigoplugin.mqttexplorerbridge/static/pages/mqtt-explorer.html?wsPort=9876
```

(Plugins → MQTT Explorer Bridge → **Open MQTT Explorer Page** logs the URL.)

The first time you open it, fill in:

- **WebSocket host**: the Indigo server's hostname/IP (e.g. `192.168.1.20`)
- **WS port**: 9876 (or whatever you configured)
- **Auth token**: matches `MQTT_EXPLORER_TOKEN` / the plugin config field
- **Use WSS**: leave unchecked

These are remembered in `localStorage`. To pre-fill the token, append
`&token=<value>` to the URL.

## Plugin actions

| Action | Description |
|---|---|
| Publish Message | Programmatic MQTT publish from Indigo schedules/triggers |
| Clear Retained Message | Sends zero-byte retained message to clear |
| Clear Local Tree / History | Resets the in-memory view (does not affect broker) |

## Limitations

- The tree is per-plugin-process — restart the plugin and it rebuilds from
  retained messages + whatever arrives.
- macOS firewall will prompt the first time `IndigoPluginHost3` binds the
  WS port. Allow it.
- The WS firehose can be heavy: zigbee2mqtt's `bridge/devices` payload alone
  is ~100KB. The coalesce window prevents the UI from choking on rapid bursts
  on the same topic, but very chatty installs should narrow the topic filter.

## Logging

Every log line is prefixed with a millisecond timestamp `[HH:MM:SS.mmm]` so
events can be correlated tightly with other CliveS plugins (Device Activity
Monitor uses the same convention).

To turn the prefix off (or back on) at any time:

**Plugins → MQTT Explorer Bridge → Toggle Timestamps in Log (on/off)**

The setting is stored in `pluginPrefs` (`timestampEnabled`) and persists across
restarts. Defaults to ON.

## Version history

- **1.0.2** (23-05-2026) — millisecond timestamp `[HH:MM:SS.mmm]` prefix on every `self.logger` line via `plugin_utils.install_timestamp_filter()`; new "Toggle Timestamps in Log" menu item.
- **1.0.1** (23-05-2026) — blanked the `host` field's `defaultValue` (was the developer's broker IP); IndigoSecrets / `MQTT_BROKER` resolution unchanged.
- **1.0.0** (18-05-2026) — initial release
