#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Filename:    plugin.py
# Description: MQTT Explorer Bridge — captures MQTT traffic and serves it to a
#              browser-based explorer via an embedded WebSocket server.
# Author:      CliveS & Claude Opus 4.7
# Date:        25-05-2026
# Version:     1.0.4
#
# v1.0.3 (25-05-2026): Cleaner WebSocket server shutdown — wait on an
# asyncio.Event instead of asyncio.sleep(3600) so the coroutine returns
# normally on stop, avoiding the "Event loop stopped before Future completed"
# RuntimeError on plugin reload.
#
# v1.0.2 (23-05-2026): Millisecond timestamp [HH:MM:SS.mmm] prefix on every
# log line via plugin_utils.install_timestamp_filter() — matches Device
# Activity Monitor convention. New "Toggle Timestamps in Log" menu item.

try:
    import indigo
except ImportError:
    pass

import asyncio
import json
import os as _os
import sys as _sys
import threading
from collections import deque
from datetime import datetime

# Plugin utils (bundled in same dir)
_sys.path.insert(0, _os.getcwd())
try:
    from plugin_utils import log_startup_banner
except ImportError:
    log_startup_banner = None
try:
    from plugin_utils import install_timestamp_filter
except ImportError:
    install_timestamp_filter = None

# Shared secrets path
_sys.path.insert(0, "/Library/Application Support/Perceptive Automation")
try:
    from IndigoSecrets import MQTT_BROKER
except ImportError:
    MQTT_BROKER = ""
try:
    from IndigoSecrets import MQTT_PORT
except ImportError:
    MQTT_PORT = 1883
try:
    from IndigoSecrets import MQTT_USERNAME
except ImportError:
    MQTT_USERNAME = ""
try:
    from IndigoSecrets import MQTT_PASSWORD
except ImportError:
    MQTT_PASSWORD = ""
try:
    from IndigoSecrets import MQTT_EXPLORER_TOKEN
except ImportError:
    MQTT_EXPLORER_TOKEN = ""

# Bundled packages (paho-mqtt, websockets)
_PACKAGES = _os.path.normpath(_os.path.join(_os.getcwd(), "..", "Packages"))
if _PACKAGES not in _sys.path:
    _sys.path.insert(0, _PACKAGES)

import paho.mqtt.client as mqtt
import websockets


# ============================================================
# Constants
# ============================================================

PLUGIN_ID      = "com.clives.indigoplugin.mqttexplorerbridge"
PLUGIN_VERSION = "1.0.4"


# ============================================================
# Helpers
# ============================================================

def log(message, level="INFO"):
    """Timestamped log — routes to Indigo event log."""
    indigo.server.log(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {message}", level=level)


def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _decode_payload(payload_bytes):
    """Return (text, is_binary). Binary payloads are hex-encoded."""
    try:
        return payload_bytes.decode("utf-8"), False
    except UnicodeDecodeError:
        return payload_bytes.hex(), True


# ============================================================
# Per-broker state
# ============================================================

class BrokerState:
    def __init__(self, dev, history_limit, topic_filters):
        self.dev_id         = dev.id
        self.name           = dev.name
        self.client         = None
        self.tree           = {}    # topic -> dict
        self.tree_lock      = threading.Lock()
        self.history_limit  = history_limit
        self.topic_filters  = topic_filters
        self.message_count  = 0
        self.dirty_counters = False  # set true on every message, flushed by WS loop


# ============================================================
# Plugin
# ============================================================

class Plugin(indigo.PluginBase):

    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs):
        super().__init__(pluginId, pluginDisplayName, pluginVersion, pluginPrefs)

        self.debug          = pluginPrefs.get("debug", False)
        self.timestamp_enabled = bool(pluginPrefs.get("timestampEnabled", True))

        if install_timestamp_filter:
            self._ts_filter = install_timestamp_filter(self, enabled=self.timestamp_enabled)
        else:
            self._ts_filter = None

        self.ws_port        = int(pluginPrefs.get("wsPort", 9876))
        self.ws_bind        = pluginPrefs.get("wsBindAddress", "0.0.0.0")
        self.coalesce_ms    = int(pluginPrefs.get("coalesceMs", 100))
        self.enable_publish = bool(pluginPrefs.get("enablePublish", True))
        self.ws_token       = pluginPrefs.get("wsToken", "") or MQTT_EXPLORER_TOKEN

        self.brokers          = {}   # dev_id -> BrokerState
        self.ws_loop          = None
        self.ws_thread        = None
        self.ws_stop_event    = None # asyncio.Event created on ws_loop, set to trigger clean shutdown
        self.ws_clients       = {}   # id(ws) -> {"ws": ws, "subs": set(dev_id)}
        self.ws_pending       = {}   # (dev_id, topic) -> entry
        self.ws_pending_lock  = threading.Lock()

        if log_startup_banner:
            log_startup_banner(pluginId, pluginDisplayName, pluginVersion, extras=[
                ("WS Port:",    str(self.ws_port)),
                ("WS Bind:",    self.ws_bind),
                ("Publish UI:", "enabled" if self.enable_publish else "READ-ONLY"),
            ])
        else:
            indigo.server.log(f"{pluginDisplayName} v{pluginVersion} starting")

        if not self.ws_token:
            self.logger.error("No WS auth token configured — set MQTT_EXPLORER_TOKEN in "
                              "IndigoSecrets.py OR 'WS Auth Token' in plugin config. "
                              "The explorer page will be unable to connect.")

    # --------------------------------------------------------
    # Lifecycle
    # --------------------------------------------------------

    def startup(self):
        self._start_ws_server()
        self.logger.info(f"{self.pluginDisplayName} started")

    def shutdown(self):
        self._stop_ws_server()
        for state in list(self.brokers.values()):
            try:
                state.client.loop_stop()
                state.client.disconnect()
            except Exception:
                pass
        self.logger.info(f"{self.pluginDisplayName} stopped")

    def closedPrefsConfigUi(self, valuesDict, userCancelled):
        if userCancelled:
            return
        new_port  = int(valuesDict.get("wsPort", 9876))
        new_bind  = valuesDict.get("wsBindAddress", "0.0.0.0")
        new_token = valuesDict.get("wsToken", "") or MQTT_EXPLORER_TOKEN
        self.coalesce_ms    = int(valuesDict.get("coalesceMs", 100))
        self.enable_publish = bool(valuesDict.get("enablePublish", True))
        self.debug          = bool(valuesDict.get("debug", False))
        self.ws_token       = new_token
        if (new_port, new_bind) != (self.ws_port, self.ws_bind):
            self.ws_port = new_port
            self.ws_bind = new_bind
            self._stop_ws_server()
            self._start_ws_server()

    # --------------------------------------------------------
    # Device lifecycle
    # --------------------------------------------------------

    def deviceStartComm(self, dev):
        if dev.deviceTypeId != "mqttBroker":
            return
        host          = dev.pluginProps.get("host") or MQTT_BROKER
        port          = int(dev.pluginProps.get("port") or MQTT_PORT)
        username      = dev.pluginProps.get("username") or MQTT_USERNAME
        password      = dev.pluginProps.get("password") or MQTT_PASSWORD
        useTLS        = bool(dev.pluginProps.get("useTLS"))
        client_id     = f"{dev.pluginProps.get('clientId') or 'indigo-mqtt-explorer'}-{dev.id}"
        hist_lim      = int(dev.pluginProps.get("historyLimit") or 50)
        topic_str     = dev.pluginProps.get("topicFilter") or "#"
        topic_filters = [t.strip() for t in topic_str.split(",") if t.strip()]

        if not host:
            self.logger.error(f"{dev.name}: no broker host (set device 'host' or "
                              f"MQTT_BROKER in IndigoSecrets.py)")
            dev.updateStateOnServer("lastError", "no host configured")
            dev.updateStateOnServer("connected", False)
            return

        state  = BrokerState(dev, hist_lim, topic_filters)
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id=client_id, clean_session=True)
        if username:
            client.username_pw_set(username, password)
        if useTLS:
            client.tls_set()
        client.reconnect_delay_set(min_delay=1, max_delay=30)

        dev_id = dev.id

        def _on_connect(c, userdata, flags, rc, props=None):
            self._on_mqtt_connect(dev_id, c, rc)

        def _on_disconnect(c, userdata, flags, rc, props=None):
            self._on_mqtt_disconnect(dev_id, rc)

        def _on_message(c, userdata, msg):
            self._on_mqtt_message(dev_id, msg)

        client.on_connect    = _on_connect
        client.on_disconnect = _on_disconnect
        client.on_message    = _on_message

        state.client = client
        self.brokers[dev.id] = state

        self.logger.info(f"{dev.name}: connecting to {host}:{port} "
                         f"(TLS={useTLS}, filter={topic_filters})")
        try:
            client.connect_async(host, port, keepalive=60)
            client.loop_start()
        except Exception as e:
            self.logger.exception(f"{dev.name}: connect_async failed: {e}")
            dev.updateStateOnServer("lastError", str(e))
            dev.updateStateOnServer("connected", False)

    def deviceStopComm(self, dev):
        state = self.brokers.pop(dev.id, None)
        if state and state.client:
            try:
                state.client.loop_stop()
                state.client.disconnect()
            except Exception:
                pass
        try:
            dev.updateStateOnServer("connected", False)
        except Exception:
            pass

    @staticmethod
    def didDeviceCommPropertyChange(oldDevice, newDevice):
        """Restart comm only when the MQTT broker connection params change.

        host/port/useTLS define the broker socket; username/password authenticate;
        clientId identifies the connection. historyLimit and topicFilter are
        re-read by the running poller and don't require a reconnect.
        """
        keys = ("host", "port", "username", "password", "useTLS", "clientId")
        return any(oldDevice.pluginProps.get(k) != newDevice.pluginProps.get(k) for k in keys)

    def deviceUpdated(self, orig_dev, new_dev):
        # Loop guard: ignore our own device updates
        super().deviceUpdated(orig_dev, new_dev)
        if new_dev.pluginId == self.pluginId:
            return

    # --------------------------------------------------------
    # MQTT callbacks (run on paho thread)
    # --------------------------------------------------------

    def _on_mqtt_connect(self, dev_id, client, rc):
        state = self.brokers.get(dev_id)
        if not state:
            return
        try:
            dev = indigo.devices[dev_id]
        except Exception:
            return
        if rc == 0:
            dev.updateStateOnServer("connected", True)
            dev.updateStateOnServer("lastError", "")
            for f in state.topic_filters:
                client.subscribe(f, qos=0)
            self.logger.info(f"{dev.name}: connected — subscribed to {state.topic_filters}")
        else:
            dev.updateStateOnServer("connected", False)
            dev.updateStateOnServer("lastError", f"connect rc={rc}")
            self.logger.error(f"{dev.name}: connect failed rc={rc}")

    def _on_mqtt_disconnect(self, dev_id, rc):
        try:
            dev = indigo.devices[dev_id]
        except Exception:
            return
        dev.updateStateOnServer("connected", False)
        if rc != 0:
            dev.updateStateOnServer("lastError", f"disconnected rc={rc}")
            if self.debug:
                self.logger.debug(f"{dev.name}: disconnected rc={rc} (will auto-reconnect)")

    def _on_mqtt_message(self, dev_id, msg):
        state = self.brokers.get(dev_id)
        if not state:
            return
        payload_str, is_binary = _decode_payload(msg.payload)
        ts = _now_iso()
        entry = {
            "topic":    msg.topic,
            "payload":  payload_str,
            "qos":      msg.qos,
            "retained": bool(msg.retain),
            "ts":       ts,
            "binary":   is_binary,
        }
        with state.tree_lock:
            node = state.tree.get(msg.topic)
            if node is None:
                node = {
                    "payload":  payload_str,
                    "qos":      msg.qos,
                    "retained": bool(msg.retain),
                    "ts":       ts,
                    "binary":   is_binary,
                    "history":  deque(maxlen=state.history_limit),
                }
                state.tree[msg.topic] = node
            else:
                node["payload"]  = payload_str
                node["qos"]      = msg.qos
                node["retained"] = bool(msg.retain)
                node["ts"]       = ts
                node["binary"]   = is_binary
            node["history"].append({
                "payload":  payload_str,
                "ts":       ts,
                "qos":      msg.qos,
                "retained": bool(msg.retain),
                "binary":   is_binary,
            })
            state.message_count += 1
        state.dirty_counters = True

        with self.ws_pending_lock:
            self.ws_pending[(dev_id, msg.topic)] = entry

    # --------------------------------------------------------
    # Snapshot / publish helpers
    # --------------------------------------------------------

    def _broker_list(self):
        out = []
        for dev_id, state in self.brokers.items():
            try:
                dev = indigo.devices[dev_id]
            except Exception:
                continue
            out.append({
                "id":           dev.id,
                "name":         dev.name,
                "host":         dev.pluginProps.get("host") or MQTT_BROKER,
                "port":         int(dev.pluginProps.get("port") or MQTT_PORT),
                "connected":    bool(dev.states.get("connected", False)),
                "topicCount":   len(state.tree),
                "messageCount": state.message_count,
            })
        return out

    def _snapshot_tree(self, dev_id):
        state = self.brokers.get(dev_id)
        if not state:
            return {}
        with state.tree_lock:
            return {
                topic: {
                    "payload":  node["payload"],
                    "qos":      node["qos"],
                    "retained": node["retained"],
                    "ts":       node["ts"],
                    "binary":   node["binary"],
                }
                for topic, node in state.tree.items()
            }

    def _topic_history(self, dev_id, topic):
        state = self.brokers.get(dev_id)
        if not state:
            return []
        with state.tree_lock:
            node = state.tree.get(topic)
            return list(node["history"]) if node else []

    def _publish(self, dev_id, topic, payload, qos, retain):
        state = self.brokers.get(dev_id)
        if not state or not state.client:
            self.logger.error(f"publish: no such broker id={dev_id}")
            return False
        if not topic:
            return False
        try:
            state.client.publish(topic, payload, qos=qos, retain=retain)
            if self.debug:
                self.logger.debug(f"{state.name}: published to {topic} (qos={qos} "
                                  f"retain={retain} len={len(payload)})")
            return True
        except Exception as e:
            self.logger.exception(f"{state.name}: publish failed: {e}")
            return False

    # --------------------------------------------------------
    # WebSocket server
    # --------------------------------------------------------

    def _start_ws_server(self):
        if self.ws_thread and self.ws_thread.is_alive():
            return
        self.ws_thread = threading.Thread(
            target=self._ws_thread_main,
            name="MQTTExplorerWS",
            daemon=True,
        )
        self.ws_thread.start()

    def _stop_ws_server(self):
        loop  = self.ws_loop
        event = self.ws_stop_event
        if loop and loop.is_running() and event is not None:
            # Signal the coroutine to return cleanly — do NOT call loop.stop(),
            # that would raise "Event loop stopped before Future completed".
            loop.call_soon_threadsafe(event.set)
        if self.ws_thread:
            self.ws_thread.join(timeout=3)
        self.ws_loop       = None
        self.ws_thread     = None
        self.ws_stop_event = None

    def _ws_thread_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.ws_loop = loop
        try:
            loop.run_until_complete(self._ws_serve_forever())
        except Exception as e:
            self.logger.exception(f"WS server crashed: {e}")
        finally:
            try:
                loop.close()
            except Exception:
                pass

    async def _ws_serve_forever(self):
        try:
            server = await websockets.serve(
                self._ws_handler, self.ws_bind, self.ws_port,
                ping_interval=20, ping_timeout=20, max_size=2_000_000,
            )
        except Exception as e:
            self.logger.error(f"WS server failed to bind {self.ws_bind}:{self.ws_port}: {e}")
            return
        self.logger.info(f"WS server listening on {self.ws_bind}:{self.ws_port}")
        flush_task = asyncio.create_task(self._ws_flush_loop())
        # Stop event MUST be created inside this coroutine so it is bound to the
        # running loop — _stop_ws_server() sets it via call_soon_threadsafe to
        # request a clean shutdown without stopping the loop.
        self.ws_stop_event = asyncio.Event()
        try:
            await self.ws_stop_event.wait()
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            flush_task.cancel()
            server.close()
            try:
                await server.wait_closed()
            except Exception:
                pass

    def _ws_path(self, ws):
        # websockets v12 → ws.path ; v13+ → ws.request.path
        req = getattr(ws, "request", None)
        if req is not None and getattr(req, "path", None):
            return req.path
        return getattr(ws, "path", "/")

    async def _ws_handler(self, ws):
        path = self._ws_path(ws)
        token_ok = False
        if "token=" in path:
            tok = path.split("token=", 1)[1].split("&", 1)[0]
            token_ok = (bool(self.ws_token) and tok == self.ws_token)
        if not token_ok:
            try:
                await ws.close(code=4401, reason="unauthorised")
            except Exception:
                pass
            return

        client_state = {"ws": ws, "subs": set()}
        key = id(ws)
        self.ws_clients[key] = client_state

        # Greet — send broker list immediately
        try:
            await ws.send(json.dumps({
                "type":           "hello",
                "brokers":        self._broker_list(),
                "publishEnabled": self.enable_publish,
            }))

            async for raw in ws:
                try:
                    req = json.loads(raw)
                except Exception:
                    continue
                action = req.get("action")

                if action == "listBrokers":
                    await ws.send(json.dumps({
                        "type":    "brokers",
                        "brokers": self._broker_list(),
                    }))

                elif action == "subscribe":
                    bid = int(req.get("brokerId", 0))
                    client_state["subs"].add(bid)
                    snap = self._snapshot_tree(bid)
                    await ws.send(json.dumps({
                        "type":     "snapshot",
                        "brokerId": bid,
                        "tree":     snap,
                    }))

                elif action == "unsubscribe":
                    bid = int(req.get("brokerId", 0))
                    client_state["subs"].discard(bid)

                elif action == "history":
                    bid   = int(req.get("brokerId", 0))
                    topic = req.get("topic", "")
                    hist  = self._topic_history(bid, topic)
                    await ws.send(json.dumps({
                        "type":     "history",
                        "brokerId": bid,
                        "topic":    topic,
                        "history":  hist,
                    }))

                elif action == "publish":
                    if not self.enable_publish:
                        await ws.send(json.dumps({
                            "type":  "error",
                            "error": "publish disabled in plugin config",
                        }))
                        continue
                    ok = self._publish(
                        int(req.get("brokerId", 0)),
                        req.get("topic", ""),
                        req.get("payload", ""),
                        int(req.get("qos", 0)),
                        bool(req.get("retain", False)),
                    )
                    await ws.send(json.dumps({
                        "type":  "published",
                        "ok":    bool(ok),
                        "topic": req.get("topic", ""),
                    }))

                elif action == "clearRetained":
                    if not self.enable_publish:
                        continue
                    self._publish(
                        int(req.get("brokerId", 0)),
                        req.get("topic", ""),
                        "",
                        0,
                        True,
                    )
        except websockets.ConnectionClosed:
            pass
        except Exception as e:
            self.logger.exception(f"WS handler error: {e}")
        finally:
            self.ws_clients.pop(key, None)

    async def _ws_flush_loop(self):
        delay = max(self.coalesce_ms, 20) / 1000.0
        while True:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return

            # Flush per-broker counters to Indigo states
            for state in list(self.brokers.values()):
                if not state.dirty_counters:
                    continue
                state.dirty_counters = False
                try:
                    dev = indigo.devices[state.dev_id]
                    dev.updateStateOnServer("messageCount",    state.message_count)
                    dev.updateStateOnServer("topicCount",      len(state.tree))
                    dev.updateStateOnServer("lastMessageTime", _now_iso())
                except Exception:
                    pass

            # Snapshot+clear pending messages
            with self.ws_pending_lock:
                if not self.ws_pending:
                    continue
                pending = self.ws_pending
                self.ws_pending = {}

            # Group by broker
            by_broker = {}
            for (dev_id, topic), entry in pending.items():
                by_broker.setdefault(dev_id, []).append(entry)

            if not self.ws_clients:
                continue

            for dev_id, entries in by_broker.items():
                msg = json.dumps({
                    "type":     "messages",
                    "brokerId": dev_id,
                    "messages": entries,
                })
                # Fan out to subscribed clients
                stale = []
                for key, cs in self.ws_clients.items():
                    if dev_id not in cs["subs"]:
                        continue
                    try:
                        await cs["ws"].send(msg)
                    except Exception:
                        stale.append(key)
                for key in stale:
                    self.ws_clients.pop(key, None)

    # --------------------------------------------------------
    # Action callbacks
    # --------------------------------------------------------

    def actionPublish(self, action, dev):
        topic   = action.props.get("topic", "")
        payload = action.props.get("payload", "")
        qos     = int(action.props.get("qos", 0))
        retain  = bool(action.props.get("retain", False))
        self._publish(dev.id, topic, payload, qos, retain)

    def actionClearRetained(self, action, dev):
        topic = action.props.get("topic", "")
        if topic:
            self._publish(dev.id, topic, "", 0, True)

    def actionClearHistory(self, action, dev):
        state = self.brokers.get(dev.id)
        if not state:
            return
        with state.tree_lock:
            state.tree.clear()
            state.message_count = 0
        dev.updateStateOnServer("topicCount",   0)
        dev.updateStateOnServer("messageCount", 0)
        self.logger.info(f"{dev.name}: tree and history cleared")

    # --------------------------------------------------------
    # Menu handlers
    # --------------------------------------------------------

    def menuOpenExplorer(self, valuesDict=None, typeId=None):
        # Print the URL to open. Page is served by Indigo IWS from this plugin's
        # static dir on port 8176 (HTTP by default). Using HTTP avoids ws://
        # mixed-content blocking. If your Indigo runs IWS in HTTPS mode you'll
        # need a TLS proxy in front of the plugin's WS server too.
        url = (f"http://{indigo.server.address}:8176/{PLUGIN_ID}"
               f"/static/pages/mqtt-explorer.html?wsPort={self.ws_port}")
        self.logger.info("MQTT Explorer URL:")
        self.logger.info(f"   {url}")
        self.logger.info("   Append &token=<your token> to pre-fill the auth field.")

    def menuDumpTree(self, valuesDict=None, typeId=None):
        for dev_id, state in self.brokers.items():
            try:
                dev = indigo.devices[dev_id]
            except Exception:
                continue
            self.logger.info(f"--- {dev.name} ({len(state.tree)} topics, "
                             f"{state.message_count} messages) ---")
            with state.tree_lock:
                for topic in sorted(state.tree.keys())[:200]:
                    node = state.tree[topic]
                    payload = node["payload"]
                    if len(payload) > 100:
                        payload = payload[:100] + "..."
                    flag = " [retained]" if node["retained"] else ""
                    self.logger.info(f"  {topic}{flag}: {payload}")
                if len(state.tree) > 200:
                    self.logger.info(f"  ... (truncated, {len(state.tree) - 200} more topics)")

    def showPluginInfo(self, valuesDict=None, typeId=None):
        extras = [
            ("WS Port:",           str(self.ws_port)),
            ("WS Bind:",           self.ws_bind),
            ("Publish UI:",        "enabled" if self.enable_publish else "READ-ONLY"),
            ("Brokers:",           str(len(self.brokers))),
            ("WS Clients:",        str(len(self.ws_clients))),
            ("Timestamps in Log:", "ON" if self.timestamp_enabled else "OFF"),
        ]
        if log_startup_banner:
            log_startup_banner(self.pluginId, self.pluginDisplayName, self.pluginVersion, extras=extras)
        else:
            indigo.server.log(f"{self.pluginDisplayName} v{self.pluginVersion}")
            for label, value in extras:
                indigo.server.log(f"  {label} {value}")

    def menuToggleTimestamps(self):
        self.timestamp_enabled = not self.timestamp_enabled
        self.pluginPrefs["timestampEnabled"] = self.timestamp_enabled
        if self._ts_filter:
            self._ts_filter.enabled = self.timestamp_enabled
        state = "ON" if self.timestamp_enabled else "OFF"
        indigo.server.log(f"[{self.pluginDisplayName}] Timestamps in Log -> {state}")
