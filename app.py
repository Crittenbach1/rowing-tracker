
import asyncio
import sqlite3
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

from bleak import BleakClient, BleakScanner
from flask import Flask, jsonify, render_template_string

DEVICE_NAME = "MRK-R28-D66F"
DEVICE_ADDRESS = "A4:C1:38:20:92:56"
ROWER_UUID = "00002ad1-0000-1000-8000-00805f9b34fb"

DB_PATH = Path(__file__).with_name("rowing.db")

IDLE_SAVE_AFTER = 12
DISCONNECT_AFTER = 120
RECONNECT_DELAY = 300

app = Flask(__name__)
lock = threading.RLock()

state = {
    "connected": False,
    "ready": False,
    "recording": False,
    "elapsed": 0,
    "strokes": 0,
    "message": "Starting Bluetooth",
}


def connect_db():
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.execute("PRAGMA busy_timeout=10000")
    return db


def initialize_db():
    with connect_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                strokes INTEGER DEFAULT 0
            )
        """)


def save_session(started, ended, seconds, strokes):
    if seconds <= 0:
        return

    with connect_db() as db:
        db.execute("""
            INSERT INTO sessions
            (started_at, ended_at, duration_seconds, strokes)
            VALUES (?, ?, ?, ?)
        """, (
            started.isoformat(),
            ended.isoformat(),
            seconds,
            strokes,
        ))

    print(f"Saved workout: {seconds} seconds", flush=True)


def reset_rower_connection():
    print("Disconnecting old Bluetooth connection...", flush=True)

    try:
        result = subprocess.run(
            ["bluetoothctl", "disconnect", DEVICE_ADDRESS],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.stdout.strip():
            print(result.stdout.strip(), flush=True)

        if result.stderr.strip():
            print(result.stderr.strip(), flush=True)

    except Exception as error:
        print("Bluetooth reset error:", error, flush=True)

    time.sleep(3)


class WorkoutTracker:
    def __init__(self):
        self.elapsed = None
        self.strokes = 0
        self.started = None
        self.last_change = time.monotonic()
        self.armed = False
        self.recording = False
        self.saved = False

        # Prevent an unchanged zero timer from
        # rearming an already saved workout.
        self.waiting_for_reset = False

    def publish(self):
        with lock:
            state["elapsed"] = self.elapsed or 0
            state["strokes"] = self.strokes
            state["ready"] = self.armed and not self.recording
            state["recording"] = self.recording

            if self.recording:
                state["message"] = "Workout in Progress"
            elif self.armed:
                state["message"] = "Connected - Ready to Row"
            elif self.saved or self.waiting_for_reset:
                state["message"] = "Workout Saved - Reset Timer"
            else:
                state["message"] = "Connected - Reset Timer"

    def on_connect(self):
        # A reconnect must not automatically save
        # the time already on the rower.
        #
        # Preserve the requirement to reset after
        # a workout has been saved.
        self.elapsed = None
        self.strokes = 0
        self.started = None
        self.armed = False
        self.recording = False
        self.saved = False
        self.last_change = time.monotonic()

        with lock:
            state["elapsed"] = 0
            state["strokes"] = 0
            state["ready"] = False
            state["recording"] = False
            state["message"] = "Connected - Reading Timer"

    def update(self, seconds, strokes):
        now = datetime.now()

        if self.elapsed is None:
            self.elapsed = seconds
            self.strokes = strokes
            self.last_change = time.monotonic()

            # An old nonzero timer is ignored.
            # A zero timer is ready only if we
            # aren't waiting for another reset.
            if seconds == 0 and not self.waiting_for_reset:
                self.armed = True

            self.publish()
            return

        previous = self.elapsed

        if seconds < previous:
            # The rower timer decreased: a reset.
            self.finish()

            self.armed = True
            self.recording = False
            self.saved = False
            self.waiting_for_reset = False
            self.started = None
            self.last_change = time.monotonic()

            print("Timer reset detected. Ready to row.", flush=True)

        elif seconds > previous:
            if self.armed and not self.saved:
                if self.started is None:
                    self.started = now

                self.recording = True
                self.last_change = time.monotonic()

        self.elapsed = seconds
        self.strokes = strokes
        self.publish()

    def finish(self):
        if (
            self.recording
            and not self.saved
            and self.started is not None
            and self.elapsed is not None
            and self.elapsed > 0
        ):
            save_session(
                self.started,
                datetime.now(),
                self.elapsed,
                self.strokes,
            )

            self.saved = True
            self.waiting_for_reset = True
            self.armed = False
            self.recording = False
            self.publish()

    def check_idle(self):
        if (
            self.recording
            and time.monotonic() - self.last_change
            >= IDLE_SAVE_AFTER
        ):
            self.finish()


tracker = WorkoutTracker()


def handle_data(sender, data):
    # FTMS rowing data: alternating packet formats.
    # The 13-byte packet includes elapsed seconds.
    if len(data) != 13:
        return

    if bytes(data[:2]) != bytes.fromhex("00 0b"):
        return

    seconds = int.from_bytes(data[11:13], "little")
    strokes = int.from_bytes(data[3:5], "little")

    tracker.update(seconds, strokes)


async def bluetooth_loop():
    while True:
        idle_disconnect = False

        try:
            with lock:
                state["connected"] = False
                state["ready"] = False
                state["recording"] = False
                state["message"] = "Searching for rower"

            print("Searching for MERACH R28...", flush=True)

            device = await BleakScanner.find_device_by_filter(
                lambda d, adv: d.name == DEVICE_NAME,
                timeout=20,
            )

            if device is None:
                print("Rower not found. Retrying...", flush=True)
                await asyncio.sleep(10)
                continue

            async with BleakClient(device) as client:
                tracker.on_connect()

                with lock:
                    state["connected"] = True

                print("MERACH R28 connected!", flush=True)

                await client.start_notify(
                    ROWER_UUID,
                    handle_data,
                )

                connected_at = time.monotonic()

                while client.is_connected:
                    tracker.check_idle()

                    last_activity = max(
                        connected_at,
                        tracker.last_change,
                    )

                    if (
                        time.monotonic() - last_activity
                        >= DISCONNECT_AFTER
                    ):
                        print(
                            "Rower idle. Disconnecting Bluetooth.",
                            flush=True,
                        )
                        idle_disconnect = True
                        break

                    await asyncio.sleep(1)

        except Exception as error:
            print("Bluetooth error:", error, flush=True)

        finally:
            tracker.finish()

            with lock:
                state["connected"] = False
                state["ready"] = False
                state["recording"] = False
                state["message"] = "Bluetooth Disconnected"

        if idle_disconnect:
            print(
                "Waiting 5 minutes before reconnecting...",
                flush=True,
            )
            await asyncio.sleep(RECONNECT_DELAY)
        else:
            await asyncio.sleep(10)


def start_bluetooth():
    asyncio.run(bluetooth_loop())


@app.route("/api/status")
def api_status():
    with lock:
        return jsonify(dict(state))


@app.route("/api/history")
def api_history():
    with connect_db() as db:
        rows = db.execute("""
            SELECT started_at, ended_at,
                   duration_seconds, strokes
            FROM sessions
            ORDER BY started_at
        """).fetchall()

    return jsonify([
        {
            "started": row[0],
            "ended": row[1],
            "seconds": row[2],
            "strokes": row[3],
        }
        for row in rows
    ])


HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport"
      content="width=device-width, initial-scale=1">
<title>My Rowing Tracker</title>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<style>
* {
    box-sizing: border-box;
}

html {
    min-height: 100%;
}

body {
    margin: 0;
    min-height: 100vh;
    padding: 30px 20px;
    font-family: Arial, sans-serif;
    color: #23364d;
    background: #f4f7fb;
    transition: background-color 0.3s ease;
}

/* Entire page turns neon green when recording. */
html.recording-active,
body.recording-active {
    background: #39FF14 !important;
}

body.recording-active .status,
body.recording-active .card,
body.recording-active .panel {
    background: #39FF14 !important;
    border: 2px solid #165e08;
}

body.recording-active h1,
body.recording-active h2,
body.recording-active h3,
body.recording-active .number,
body.recording-active .status-title {
    color: #103b08;
}

body.recording-active .status-detail {
    color: #235e13;
}

body.recording-active button {
    background: #b8ff9f;
    border-color: #165e08;
    color: #103b08;
}

body.recording-active button.active {
    background: #165e08;
    color: white;
}

.container {
    max-width: 950px;
    margin: auto;
}

h1 {
    color: #245e96;
    margin-bottom: 24px;
}

.status {
    padding: 20px;
    border-radius: 12px;
    background: white;
    margin-bottom: 20px;
    border-left: 6px solid #888;
}

.status.ready {
    border-left-color: #27ae60;
}

.status.recording {
    border-left-color: #3287cb;
}

.status.waiting {
    border-left-color: #e0a12d;
}

.status-title {
    font-size: 21px;
    font-weight: bold;
}

.status-detail {
    color: #68788b;
    margin-top: 8px;
}

.cards {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
}

.card {
    background: white;
    border-radius: 12px;
    padding: 20px;
    flex: 1;
    min-width: 150px;
}

.card p {
    margin-top: 0;
}

.number {
    font-size: 32px;
    font-weight: bold;
}

.panel {
    background: white;
    border-radius: 12px;
    padding: 20px;
    margin-top: 20px;
}

button {
    background: white;
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 10px 15px;
    margin: 3px;
    cursor: pointer;
}

button.active {
    background: #245e96;
    color: white;
}

.chart-wrap {
    position: relative;
    width: 100%;
    height: 340px;
}

@media (max-width: 600px) {
    body {
        padding: 15px 10px;
    }

    .number {
        font-size: 27px;
    }

    .chart-wrap {
        height: 270px;
    }
}
</style>
</head>

<body>
<div class="container">

<h1>My Rowing Tracker</h1>

<div class="status" id="statusBox">
    <div class="status-title" id="statusTitle">
        Searching for rower...
    </div>
    <div class="status-detail" id="statusDetail">
        Waiting for Bluetooth
    </div>
</div>

<div class="cards">
    <div class="card">
        <p>Rower Timer</p>
        <div class="number" id="current">0:00</div>
    </div>

    <div class="card">
        <p>Today</p>
        <div class="number" id="today">0 min</div>
    </div>

    <div class="card">
        <p>All Time</p>
        <div class="number" id="total">0 min</div>
    </div>
</div>

<div class="panel">
    <h2>Rowing History</h2>

    <div>
        <button data-period="day"
                onclick="changePeriod('day')">Day</button>
        <button data-period="week"
                onclick="changePeriod('week')">Week</button>
        <button data-period="month"
                onclick="changePeriod('month')">Month</button>
        <button data-period="year"
                onclick="changePeriod('year')">Year</button>
        <button data-period="all"
                onclick="changePeriod('all')">All Time</button>
    </div>

    <h3 id="periodTotal">0 minutes</h3>

    <div class="chart-wrap">
        <canvas id="chart"></canvas>
    </div>
</div>

</div>

<script>
let period = "year";
let chart = null;
let sessions = [];

function dateKey(d) {
    return d.getFullYear() + "-" +
        String(d.getMonth() + 1).padStart(2, "0") + "-" +
        String(d.getDate()).padStart(2, "0");
}

function changePeriod(value) {
    period = value;

    document.querySelectorAll("button[data-period]")
        .forEach(button => {
            button.classList.toggle(
                "active",
                button.dataset.period === value
            );
        });

    drawChart();
}

function drawChart() {
    const now = new Date();

    let count = 24;

    if (period === "week") count = 7;

    if (period === "month") {
        count = new Date(
            now.getFullYear(),
            now.getMonth() + 1,
            0
        ).getDate();
    }

    if (period === "year") count = 12;

    const years = sessions.map(
        s => new Date(s.started).getFullYear()
    );

    const firstYear = years.length
        ? Math.min(...years)
        : now.getFullYear();

    if (period === "all") {
        count = now.getFullYear() - firstYear + 1;
    }

    const buckets = Array(count).fill(0);
    const labels = Array(count).fill("");

    const weekStart = new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate()
    );

    weekStart.setDate(
        weekStart.getDate() -
        ((weekStart.getDay() + 6) % 7)
    );

    sessions.forEach(s => {
        const d = new Date(s.started);
        let index = -1;

        if (
            period === "day" &&
            dateKey(d) === dateKey(now)
        ) {
            index = d.getHours();
        }

        if (period === "week") {
            const sessionDay = new Date(
                d.getFullYear(),
                d.getMonth(),
                d.getDate()
            );

            index = Math.round(
                (sessionDay - weekStart) / 86400000
            );
        }

        if (
            period === "month" &&
            d.getMonth() === now.getMonth() &&
            d.getFullYear() === now.getFullYear()
        ) {
            index = d.getDate() - 1;
        }

        if (
            period === "year" &&
            d.getFullYear() === now.getFullYear()
        ) {
            index = d.getMonth();
        }

        if (period === "all") {
            index = d.getFullYear() - firstYear;
        }

        if (index >= 0 && index < count) {
            buckets[index] += s.seconds / 60;
        }
    });

    for (let i = 0; i < count; i++) {
        if (period === "day") {
            labels[i] = i + ":00";
        }

        if (period === "week") {
            labels[i] = [
                "Mon", "Tue", "Wed", "Thu",
                "Fri", "Sat", "Sun"
            ][i];
        }

        if (period === "month") {
            labels[i] = String(i + 1);
        }

        if (period === "year") {
            labels[i] = [
                "Jan", "Feb", "Mar", "Apr",
                "May", "Jun", "Jul", "Aug",
                "Sep", "Oct", "Nov", "Dec"
            ][i];
        }

        if (period === "all") {
            labels[i] = String(firstYear + i);
        }
    }

    const sum = buckets.reduce((a, b) => a + b, 0);

    document.getElementById("periodTotal").textContent =
        sum.toFixed(1) + " minutes";

    if (chart) chart.destroy();

    const recording =
        document.body.classList.contains("recording-active");

    chart = new Chart(
        document.getElementById("chart"),
        {
            type: "bar",
            data: {
                labels: labels,
                datasets: [{
                    label: "Minutes rowed",
                    data: buckets,
                    backgroundColor: recording
                        ? "#165e08"
                        : "#3287cb",
                    borderRadius: 5
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: { beginAtZero: true }
                }
            }
        }
    );
}

async function refresh() {
    try {
        const statusResponse = await fetch("/api/status");
        const status = await statusResponse.json();

        const recording =
            status.connected && status.recording;

        // Bright green page while recording.
        document.body.classList.toggle(
            "recording-active",
            recording
        );

        document.documentElement.classList.toggle(
            "recording-active",
            recording
        );

        const box = document.getElementById("statusBox");
        const title = document.getElementById("statusTitle");
        const detail = document.getElementById("statusDetail");

        box.className = "status";

        if (!status.connected) {
            title.textContent = status.message;
            detail.textContent = "Not recording";

        } else if (recording) {
            box.classList.add("recording");
            title.textContent = "Workout in Progress";
            detail.textContent = "Recording your rowing time";

        } else if (status.ready) {
            box.classList.add("ready");
            title.textContent = "Connected - Ready to Row";
            detail.textContent =
                "Your next workout will be recorded";

        } else {
            box.classList.add("waiting");
            title.textContent = status.message;
            detail.textContent =
                "Reset the rower timer before your next workout";
        }

        const seconds = status.elapsed || 0;

        document.getElementById("current").textContent =
            Math.floor(seconds / 60) + ":" +
            String(seconds % 60).padStart(2, "0");

        const historyResponse = await fetch("/api/history");
        sessions = await historyResponse.json();

        const now = new Date();

        const total = sessions.reduce(
            (sum, s) => sum + s.seconds,
            0
        );

        const today = sessions
            .filter(
                s => dateKey(new Date(s.started)) === dateKey(now)
            )
            .reduce(
                (sum, s) => sum + s.seconds,
                0
            );

        document.getElementById("today").textContent =
            (today / 60).toFixed(1) + " min";

        document.getElementById("total").textContent =
            (total / 60).toFixed(1) + " min";

        drawChart();

    } catch (error) {
        console.error("Dashboard update error:", error);
    }
}

changePeriod("year");
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


if __name__ == "__main__":
    initialize_db()

    # Clear an existing Bluetooth connection
    # before searching for the rower.
    reset_rower_connection()

    thread = threading.Thread(
        target=start_bluetooth,
        daemon=True,
    )
    thread.start()

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
    )
