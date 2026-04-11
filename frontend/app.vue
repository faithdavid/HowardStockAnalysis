                                                                                                                                                                                                                                                   <template>
  <div class="dashboard-container">
    <div class="header">
      <div>
        <h1>Howard Stock Analysis</h1>
        <p style="color: var(--text-muted); margin-top: 8px;">Automated MGPR Technical & Insider Tracker</p>
      </div>
      <div style="display: flex; align-items: center; gap: 16px;">
        <div v-if="serverStatus === 'online'" class="status-badge" :style="{ background: Math.abs(spyGap) > 0.5 ? 'var(--danger)' : 'rgba(255,255,255,0.1)' }">
          SPY: {{ spyGap > 0 ? '+' : '' }}{{ spyGap.toFixed(2) }}% 
          <span v-if="Math.abs(spyGap) > 0.5" style="margin-left: 4px;">⚠️ High Volatility</span>
        </div>
        <span v-if="serverStatus === 'online'" class="status-badge status-online">API Online</span>
        <span v-else class="status-badge status-offline">API Offline</span>
        <button class="btn" style="padding: 6px 14px; font-size: 13px; background-color: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.2);" @click="openSettings">
          ⚙️ Config
        </button>
      </div>
    </div>

    <div class="grid-2">
      <!-- System Status Card -->
      <div class="card">
        <h2>System Status</h2>
        <div style="margin-top: 20px;">
          <p><strong>Next Automated Run:</strong> 7:00 AM EST (Mon-Fri)</p>
          <h3 style="margin-top: 16px; font-size: 14px; color: var(--text-muted);">Recent Pipeline Runs</h3>
          <div style="margin-top: 8px; max-height: 250px; overflow-y: auto; padding-right: 4px;">
            <div v-for="(run, idx) in historyData" :key="idx" style="background: rgba(0,0,0,0.2); padding: 12px; border-radius: 8px; margin-bottom: 8px; font-size: 13px;">
              <div style="display: flex; justify-content: space-between;">
                <strong>{{ new Date(run.time).toLocaleString() }}</strong>
                <span :style="{color: run.status.includes('error') ? 'var(--danger)' : 'var(--success)', fontWeight: 'bold'}">{{ run.status.toUpperCase() }}</span>
              </div>
              <div style="margin-top: 4px; color: #ccc;">
                {{ run.signals }} Signals | {{ run.duration_sec || 0 }}s runtime | {{ run.message }}
              </div>
            </div>
            <p v-if="historyData.length === 0" style="color: var(--text-muted); font-size: 13px;">No history recorded yet.</p>
          </div>
        </div>
      </div>

      <!-- Control Panel Card -->
      <div class="card">
        <h2>Manual Override</h2>
        <p style="margin-top: 12px; color: var(--text-muted); line-height: 1.5;">
          Scrape OpenInsider, fetch Polygon data, and run TradingView CAD market technicals on demand.
        </p>
        
        <div style="margin-top: 24px; display: flex; flex-direction: column; gap: 12px;">
          <button @click="triggerRun" :disabled="isRunning || isHealthChecking || serverStatus === 'offline'" class="btn" :class="{'btn-pulse': isRunning}">
            <span v-if="isRunning">🔄 Pipeline Active (Fetching live...)</span>
            <span v-else>🚀 Trigger Full Pipeline Now</span>
          </button>

          <div v-if="isRunning || isHealthChecking" class="live-console">
            <div v-for="(log, idx) in liveLogs" :key="idx" class="log-line">>> {{ log }}</div>
            <div class="log-cursor">_</div>
          </div>

          <div style="margin-top: 16px; margin-bottom: 8px;">
            <label style="font-size: 12px; color: var(--text-muted); font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;">
              History Lookback Window
            </label>
            <div class="selector-group">
              <button v-for="d in [7, 14, 30, 60, 90]" :key="d" 
                      class="selector-btn" :class="{'active': lookbackDays === d}"
                      @click="lookbackDays = d">
                {{ d }}d
              </button>
            </div>
          </div>

          <button @click="triggerHealthCheck" :disabled="isRunning || isHealthChecking || serverStatus === 'offline'" class="btn btn-secondary" :class="{'btn-pulse': isHealthChecking}">
            <span v-if="isHealthChecking">Analyzing {{ lookbackDays }} Days... (1-2 mins)</span>
            <span v-else>🔍 Run {{ lookbackDays }}-Day Strategic Health Check</span>
          </button>
        </div>
      </div>
    </div>

    <!-- Health Check Results -->
    <div class="card" style="margin-top: 24px;" v-if="healthCheckResults && healthCheckResults.length > 0">
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <h2>Strategic Health Check (Last 30 Days)</h2>
        <span class="status-badge" style="background: rgba(255,255,255,0.1);">{{ healthCheckPeriod }}</span>
      </div>
      <div class="table-wrapper" style="margin-top: 16px;">
        <table>
          <thead>
            <tr>
              <th>Strategy Module</th>
              <th>Total Trades</th>
              <th>Win Rate</th>
              <th>Avg Return</th>
              <th>Total Return</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="res in healthCheckResults" :key="res.module">
              <td style="font-weight: 600; color: #fff;">{{ res.module }}</td>
              <td>{{ res.trades }}</td>
              <td :class="res.win_rate >= 55 ? 'score-high' : 'score-med'">{{ res.win_rate }}%</td>
              <td :style="{color: res.avg_return > 0 ? 'var(--success)' : 'var(--danger)'}">{{ res.avg_return > 0 ? '+' : '' }}{{ res.avg_return }}%</td>
              <td :style="{color: res.total_return > 0 ? 'var(--success)' : 'var(--danger)'}" style="font-weight: bold;">
                {{ res.total_return > 0 ? '+' : '' }}{{ res.total_return }}%
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p style="margin-top: 16px; font-size: 12px; color: var(--text-muted);">
        * Backtest results are pushed to the "Historical/Backtest" table in Airtable for long-term tracking.
      </p>
    </div>

    <!-- Recent Signals Table -->
    <div class="card" style="margin-top: 24px;" v-if="recentSignals && recentSignals.length > 0">
      <h2>Recent Signals Discovered</h2>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Variant</th>
              <th>Score</th>
              <th>Entry</th>
              <th>Stop Loss</th>
              <th>Take Profit</th>
              <th>Rationale</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(s, index) in recentSignals" :key="s.ticker + index">
              <td style="font-weight: 600; color: #fff;">{{ s.ticker }}</td>
              <td>
                <span class="status-badge" :style="{ background: s.variant === 'V1' ? 'rgba(52, 199, 89, 0.2)' : 'rgba(0, 122, 255, 0.2)', color: s.variant === 'V1' ? '#34c759' : '#007aff', border: '1px solid currentColor', fontSize: '11px', padding: '2px 8px' }">
                  {{ s.variant }}
                </span>
              </td>
              <td :class="s.score >= 80 ? 'score-high' : (s.score >= 60 ? 'score-med' : 'score-low')">{{ s.score }}/100</td>
              <td>${{ Number(s.entry).toFixed(2) }}</td>
              <td>${{ Number(s.stop).toFixed(2) }}</td>
              <td>{{ s.take_profit ? '$' + Number(s.take_profit).toFixed(2) : 'Hold to Close' }}</td>
              <td style="font-size: 13px; color: var(--text-muted);">{{ s.rationale }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Settings Modal -->
    <div class="modal-overlay" v-if="showSettings" @click.self="showSettings = false">
      <div class="modal">
        <div class="modal-header">
          <h2>Scanner Parameters</h2>
          <button class="modal-close" @click="showSettings = false">&times;</button>
        </div>
        <div class="modal-body">
          <p style="margin-bottom: 20px; color: var(--text-muted); font-size: 13px;">
            Configure MGPR and Technical thresholds. These save securely directly to your backend.
          </p>

          <div class="form-group">
            <label>Min Insider MGPR to Alert (Howard: 85+)</label>
            <input type="number" class="form-control" v-model="settingsData.MIN_SCORE_FOR_ALERT" />
          </div>
          
          <div class="form-group">
            <label>Min Technical Scan Score (Howard: 50+)</label>
            <input type="number" class="form-control" v-model="settingsData.MIN_SCAN_SCORE" />
          </div>

          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
            <div class="form-group">
              <label>V1 Min ATR % (Book: 3.5)</label>
              <input type="number" step="0.1" class="form-control" v-model="settingsData.V1_ATR_MIN" />
            </div>
            <div class="form-group">
              <label>Repeat Buy Days (Book: 30)</label>
              <input type="number" class="form-control" v-model="settingsData.REPEAT_BUY_DAYS" />
            </div>
          </div>

          <button class="btn" style="width: 100%; justify-content: center; margin-top: 10px;" @click="saveSettings">
            Save Settings
          </button>
        </div>
      </div>
    </div>

  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

const serverStatus = ref('offline')
const lastRun = ref(null)
const isRunning = ref(false)
const liveLogs = ref([])
const isHealthChecking = ref(false)
const lookbackDays = ref(30)
const recentSignals = ref([])
const healthCheckResults = ref([])
const healthCheckPeriod = ref('')
const historyData = ref([])
const spyGap = ref(0.0)

// Settings modal state
const showSettings = ref(false)
const settingsData = ref({
  MIN_SCORE_FOR_ALERT: '85',
  MIN_SCAN_SCORE: '50',
  V1_ATR_MIN: '3.5',
  REPEAT_BUY_DAYS: '30'
})

const API_BASE = useRuntimeConfig().public.apiBase
const RUN_SECRET = useRuntimeConfig().public.runSecret

const loadingPhases = [
  "Connecting to Exchange servers...",
  "Fetching OpenInsider Filings...",
  "Retrieving Polygon Options Data...",
  "Evaluating MGPR algorithms...",
  "Running Technical Market Scans...",
  "Validating and writing to AirTable..."
]

const checkStatus = async () => {
  try {
    const res = await fetch(`${API_BASE}/`)
    if (res.ok) {
      serverStatus.value = 'online'
      const data = await res.json()
      lastRun.value = data.last_run
      spyGap.value = data.spy_gap || 0.0
    } else {
      serverStatus.value = 'offline'
    }
  } catch (err) {
    serverStatus.value = 'offline'
  }

  // Also fetch history
  try {
    const histRes = await fetch(`${API_BASE}/history`)
    if (histRes.ok) {
      historyData.value = await histRes.json()
    }
  } catch (err) {
    console.error("Failed to fetch history")
  }
}

const pollRunStatus = async () => {
  if (!isRunning.value) return;
  try {
    const res = await fetch(`${API_BASE}/run-status`)
    if (res.ok) {
      const data = await res.json()
      liveLogs.value = data.logs || []
      
      if (!data.is_running && liveLogs.value.length > 0) {
        // Just finished!
        isRunning.value = false
        setTimeout(checkStatus, 1000)
      } else if (data.is_running) {
        // Keep polling
        setTimeout(pollRunStatus, 1000)
      }
    }
  } catch (err) {
    console.error("Status poll failed:", err)
    setTimeout(pollRunStatus, 2000)
  }
}

const triggerRun = async () => {
  if (!confirm("Trigger a manual scan? The logs will stream live below.")) return
  
  isRunning.value = true
  recentSignals.value = []
  liveLogs.value = []

  try {
    const res = await fetch(`${API_BASE}/run`, {
      method: 'POST',
      headers: { 'X-Run-Secret': RUN_SECRET }
    })
    if (res.ok) {
      // It immediately returns started. Now we poll for logs.
      pollRunStatus()
    } else {
      const errData = await res.json()
      console.error("Pipeline failure detail:", errData.detail)
      alert("Pipeline failed. Check console for details.")
      isRunning.value = false
    }
  } catch (err) {
    console.error("Network error:", err)
    alert("Network error. Is the backend running?")
    isRunning.value = false
  }
}

const pollHealthStatus = async () => {
  if (!isHealthChecking.value) return;
  try {
    const res = await fetch(`${API_BASE}/health-status`)
    if (res.ok) {
      const data = await res.json()
      liveLogs.value = data.logs || []
      
      if (!data.is_running && liveLogs.value.length > 0) {
        // Just finished!
        isHealthChecking.value = false
        healthCheckResults.value = data.results || []
        setTimeout(checkStatus, 1000)
      } else if (data.is_running) {
        // Keep polling
        setTimeout(pollHealthStatus, 1000)
      }
    }
  } catch (err) {
    console.error("Health status poll failed:", err)
    setTimeout(pollHealthStatus, 2000)
  }
}

const triggerHealthCheck = async () => {
  const confirmMsg = `Run ${lookbackDays.value}-day strategy health check? The terminal will show progress.`
  if (!confirm(confirmMsg)) return
  
  isHealthChecking.value = true
  healthCheckResults.value = []
  liveLogs.value = []

  try {
    // Send lookback_days as query param
    const res = await fetch(`${API_BASE}/health-check?lookback_days=${lookbackDays.value}`, {
      method: 'POST',
      headers: { 'X-Run-Secret': RUN_SECRET }
    })
    if (res.ok) {
      pollHealthStatus()
    } else {
      const errData = await res.json()
      console.error("Health check error:", errData.detail)
      alert("Health check failed. Check console.")
      isHealthChecking.value = false
    }
  } catch (err) {
    console.error("Network error:", err)
    alert("Network error during health check.")
    isHealthChecking.value = false
  }
}

const openSettings = async () => {
  try {
    const res = await fetch(`${API_BASE}/settings`)
    if (res.ok) {
      const data = await res.json()
      // Merge with defaults
      settingsData.value = { ...settingsData.value, ...data }
    }
  } catch (err) {
    console.error("Could not fetch settings", err)
  }
  showSettings.value = true
}

const saveSettings = async () => {
  try {
    const res = await fetch(`${API_BASE}/settings`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(settingsData.value)
    })
    if (res.ok) {
      showSettings.value = false
      alert("Settings successfully saved and live!")
    } else {
      const errData = await res.json()
      console.error("Settings save error:", errData.detail)
      alert("Failed to save settings. See console for error.")
    }
  } catch (err) {
    console.error("Network error saving settings:", err)
    alert("Network error. Could not reach backend.")
  }
}

onMounted(() => {
  checkStatus()
  setInterval(checkStatus, 10000)
})
</script>
