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
        """, (started.isoformat(), ended.isoformat(), seconds, strokes))

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
        # An existing rower timer reading must not be saved again on reconnect.
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
            if seconds == 0 and not self.waiting_for_reset:
                self.armed = True
            self.publish()
            return

        previous = self.elapsed

        if seconds < previous:
            # The rower timer decreased, so the user reset it.
            self.finish()
            self.armed = True
            self.recording = False
            self.saved = False
            self.waiting_for_reset = False
            self.started = None
            self.last_change = time.monotonic()
            print("Timer reset detected. Ready to row.", flush=True)

        elif seconds > previous and self.armed and not self.saved:
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
            and time.monotonic() - self.last_change >= IDLE_SAVE_AFTER
        ):
            self.finish()


tracker = WorkoutTracker()


def handle_data(sender, data):
    # The 13-byte FTMS packet contains elapsed seconds.
    if len(data) != 13 or bytes(data[:2]) != bytes.fromhex("00 0b"):
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
                lambda device, advertisement: device.name == DEVICE_NAME,
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
                await client.start_notify(ROWER_UUID, handle_data)

                connected_at = time.monotonic()
                while client.is_connected:
                    tracker.check_idle()
                    last_activity = max(connected_at, tracker.last_change)

                    if time.monotonic() - last_activity >= DISCONNECT_AFTER:
                        print("Rower idle. Disconnecting Bluetooth.", flush=True)
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
            print("Waiting 5 minutes before reconnecting...", flush=True)
            await asyncio.sleep(RECONNECT_DELAY)
        else:
            await asyncio.sleep(10)


def start_bluetooth():
    asyncio.run(bluetooth_loop())


@app.route("/api/status")
def api_status():
    with lock:
        status = dict(state)

    with connect_db() as db:
        revision = db.execute("SELECT MAX(id) FROM sessions").fetchone()[0]

    status["history_revision"] = revision or 0
    return jsonify(status)


@app.route("/api/history")
def api_history():
    with connect_db() as db:
        rows = db.execute("""
            SELECT started_at, ended_at, duration_seconds, strokes
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
<meta name="viewport" content="width=device-width, initial-scale=1">
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

html.recording-active,
body.recording-active {
    background: #39ff14 !important;
}

body.recording-active .status,
body.recording-active .card,
body.recording-active .panel {
    background: #39ff14 !important;
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

.status,
.card,
.panel {
    background: white;
    border-radius: 12px;
    padding: 20px;
}

.status {
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

.goal-progress {
    display: block;
    width: 100%;
    height: 18px;
    margin: 12px 0;
    appearance: none;
    border: 0;
    border-radius: 999px;
    overflow: hidden;
    background: #e4eaf0;
    accent-color: #1a7f37;
}

.goal-progress::-webkit-progress-bar {
    background: #e4eaf0;
}

.goal-progress::-webkit-progress-value {
    background: #1a7f37;
}

.goal-progress::-moz-progress-bar {
    background: #1a7f37;
}

.calendar-scroll {
    overflow-x: auto;
}

.calendar-grid {
    display: grid;
    grid-template-rows: 18px repeat(7, 12px);
    grid-auto-columns: 12px;
    gap: 3px;
    width: max-content;
    padding: 0 20px 6px 0;
}

.calendar-month {
    font-size: 12px;
    white-space: nowrap;
}

.calendar-day {
    width: 12px;
    height: 12px;
    border-radius: 2px;
    background: #e4eaf0;
}

.calendar-day.complete {
    background: #1a7f37;
}

.calendar-legend {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    font-size: 13px;
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
    <h1>My Rowing Tracker - Test</h1>

    <div class="status" id="statusBox">
        <div class="status-title" id="statusTitle">Searching for rower...</div>
        <div class="status-detail" id="statusDetail">Waiting for Bluetooth</div>
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

    <section class="panel" aria-labelledby="goalHeading">
        <h2 id="goalHeading">Daily Rowing Goal</h2>
        <progress class="goal-progress" id="goalProgress"
                  aria-labelledby="goalHeading" aria-describedby="goalDetail"
                  max="1800" value="0"></progress>
        <div id="goalDetail">Loading today's rowing time…</div>
    </section>

    <div class="panel">
        <h2>Rowing History</h2>
        <div>
            <button data-period="day" onclick="changePeriod('day')">Day</button>
            <button data-period="week" onclick="changePeriod('week')">Week</button>
            <button data-period="month" onclick="changePeriod('month')">Month</button>
            <button data-period="year" onclick="changePeriod('year')">Year</button>
            <button data-period="all" onclick="changePeriod('all')">All Time</button>
        </div>
        <h3 id="periodTotal">0 minutes</h3>
        <div class="chart-wrap">
            <canvas id="chart"></canvas>
        </div>
    </div>

    <section class="panel" aria-labelledby="calendarHeading">
        <h2 id="calendarHeading">Daily Goal Calendar</h2>
        <p id="calendarSummary"></p>
        <div class="calendar-scroll" tabindex="0" aria-labelledby="calendarHeading">
            <div id="goalCalendar" class="calendar-grid" role="group"
                 aria-label="Daily goal results by week, Monday through Sunday"></div>
        </div>
        <p class="calendar-legend">
            <span class="calendar-day" aria-hidden="true"></span> Below 30 min
            <span class="calendar-day complete" aria-hidden="true"></span> 30 min or more
        </p>
    </section>
</div>

<script>
let period = "year";
let chart = null;
let sessions = [];
let historyRevision = null;
let renderedDay = null;

const GOAL_SECONDS = 30 * 60;
const monthNames = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
];
const calendarDate = new Intl.DateTimeFormat("en-US", { dateStyle: "full" });

function formatDuration(seconds) {
    return Math.floor(seconds / 60) + ":" +
        String(seconds % 60).padStart(2, "0");
}

function dateKey(day) {
    return day.getFullYear() + "-" +
        String(day.getMonth() + 1).padStart(2, "0") + "-" +
        String(day.getDate()).padStart(2, "0");
}

function changePeriod(value) {
    period = value;

    document.querySelectorAll("button[data-period]").forEach(button => {
        button.classList.toggle("active", button.dataset.period === value);
    });

    drawChart();
}

function drawChart(now = new Date()) {
    let count = 24;
    if (period === "week") count = 7;
    if (period === "month") {
        count = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    }
    if (period === "year") count = 12;

    const years = sessions.map(session => new Date(session.started).getFullYear());
    const firstYear = years.length ? Math.min(...years) : now.getFullYear();
    if (period === "all") {
        count = now.getFullYear() - firstYear + 1;
    }

    const buckets = Array(count).fill(0);
    const labels = Array(count).fill("");
    const weekStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    weekStart.setDate(weekStart.getDate() - ((weekStart.getDay() + 6) % 7));

    for (const session of sessions) {
        const day = new Date(session.started);
        let index = -1;

        if (period === "day" && dateKey(day) === dateKey(now)) {
            index = day.getHours();
        }

        if (period === "week") {
            const sessionDay = new Date(day.getFullYear(), day.getMonth(), day.getDate());
            index = Math.round((sessionDay - weekStart) / 86400000);
        }

        if (
            period === "month" &&
            day.getMonth() === now.getMonth() &&
            day.getFullYear() === now.getFullYear()
        ) {
            index = day.getDate() - 1;
        }

        if (period === "year" && day.getFullYear() === now.getFullYear()) {
            index = day.getMonth();
        }

        if (period === "all") {
            index = day.getFullYear() - firstYear;
        }

        if (index >= 0 && index < count) {
            buckets[index] += session.seconds / 60;
        }
    }

    for (let i = 0; i < count; i++) {
        if (period === "day") {
            labels[i] = (i % 12 || 12) + (i < 12 ? " AM" : " PM");
        }
        if (period === "week") {
            labels[i] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][i];
        }
        if (period === "month") labels[i] = String(i + 1);
        if (period === "year") labels[i] = monthNames[i];
        if (period === "all") labels[i] = String(firstYear + i);
    }

    const sum = buckets.reduce((total, minutes) => total + minutes, 0);
    document.getElementById("periodTotal").textContent =
        sum.toFixed(1) + " minutes";

    if (chart) {
        chart.data.labels = labels;
        chart.data.datasets[0].data = buckets;
        chart.update("none");
        return;
    }

    chart = new Chart(document.getElementById("chart"), {
        type: "bar",
        data: {
            labels,
            datasets: [{
                label: "Minutes rowed",
                data: buckets,
                backgroundColor: "#3287cb",
                borderRadius: 5
            }]
        },
        options: {
            animation: false,
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { beginAtZero: true }
            }
        }
    });
}

function renderCalendar(now, secondsByDay) {
    const year = now.getFullYear();
    const firstDay = new Date(year, 0, 1);
    const leadingDays = (firstDay.getDay() + 6) % 7;
    const cells = document.createDocumentFragment();

    let daysCompleted = 0;
    let dayCount = 0;

    for (
        const day = new Date(firstDay);
        day.getFullYear() === year;
        day.setDate(day.getDate() + 1)
    ) {
        const offset = leadingDays + dayCount;
        const column = Math.floor(offset / 7) + 1;
        const seconds = secondsByDay.get(dateKey(day)) || 0;
        const complete = seconds >= GOAL_SECONDS;

        dayCount++;
        if (complete) daysCompleted++;

        if (day.getDate() === 1) {
            const month = document.createElement("span");
            month.className = "calendar-month";
            month.textContent = monthNames[day.getMonth()];
            month.style.gridColumn = column;
            month.style.gridRow = 1;
            month.setAttribute("aria-hidden", "true");
            cells.append(month);
        }

        const cell = document.createElement("span");
        cell.className = complete ? "calendar-day complete" : "calendar-day";
        cell.style.gridColumn = column;
        cell.style.gridRow = offset % 7 + 2;
        cell.title = calendarDate.format(day) + " · " +
            formatDuration(seconds) + " rowed" +
            (complete ? " · Goal met" : "");
        cell.setAttribute("role", "img");
        cell.setAttribute("aria-label", cell.title);
        cells.append(cell);
    }

    document.getElementById("goalCalendar").replaceChildren(cells);
    document.getElementById("calendarHeading").textContent =
        year + " Daily Goal Calendar";
    document.getElementById("calendarSummary").textContent =
        "Goal met on " + daysCompleted + " of " + dayCount + " days";
}

function renderHistory(now) {
    const secondsByDay = new Map();
    let total = 0;

    for (const session of sessions) {
        const day = dateKey(new Date(session.started));
        secondsByDay.set(day, (secondsByDay.get(day) || 0) + session.seconds);
        total += session.seconds;
    }

    const today = secondsByDay.get(dateKey(now)) || 0;
    document.getElementById("today").textContent =
        (today / 60).toFixed(1) + " min";
    document.getElementById("total").textContent =
        (total / 60).toFixed(1) + " min";

    document.getElementById("goalProgress").value = Math.min(today, GOAL_SECONDS);
    document.getElementById("goalDetail").textContent =
        formatDuration(today) + " / " + formatDuration(GOAL_SECONDS) +
        (today >= GOAL_SECONDS ? " · Goal complete" : "");

    renderCalendar(now, secondsByDay);
    drawChart(now);
}

async function refresh() {
    try {
        const statusResponse = await fetch("/api/status", { cache: "no-store" });
        if (!statusResponse.ok) throw new Error("Could not load rower status");
        const status = await statusResponse.json();
        const recording = status.connected && status.recording;

        document.body.classList.toggle("recording-active", recording);
        document.documentElement.classList.toggle("recording-active", recording);

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
            detail.textContent = "Your next workout will be recorded";
        } else {
            box.classList.add("waiting");
            title.textContent = status.message;
            detail.textContent = "Reset the rower timer before your next workout";
        }

        document.getElementById("current").textContent =
            formatDuration(status.elapsed || 0);

        // Download history only on the first load or after a new session is saved.
        const historyChanged = status.history_revision !== historyRevision;
        if (historyChanged) {
            const historyResponse = await fetch("/api/history", { cache: "no-store" });
            if (!historyResponse.ok) throw new Error("Could not load rowing history");
            sessions = await historyResponse.json();
        }

        const now = new Date();
        const todayKey = dateKey(now);
        if (historyChanged || todayKey !== renderedDay) {
            renderHistory(now);
            renderedDay = todayKey;
            historyRevision = status.history_revision;
        }
    } catch (error) {
        console.error("Dashboard update error:", error);
    } finally {
        setTimeout(refresh, 2000);
    }
}

changePeriod("year");
refresh();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML)


if __name__ == "__main__":
    initialize_db()
    reset_rower_connection()

    thread = threading.Thread(target=start_bluetooth, daemon=True)
    thread.start()

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        use_reloader=False,
    )
