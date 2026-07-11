// If you want to use Phoenix channels, run `mix help phx.gen.channel`
// to get started and then uncomment the line below.
// import "./user_socket.js"

// You can include dependencies in two ways.
//
// The simplest option is to put them in assets/vendor and
// import them using relative paths:
//
//     import "../vendor/some-package.js"
//
// Alternatively, you can `npm install some-package --prefix assets` and import
// them using a path starting with the package name:
//
//     import "some-package"
//
// If you have dependencies that try to import CSS, esbuild will generate a separate `app.css` file.
// To load it, simply add a second `<link>` to your `root.html.heex` file.

// Include phoenix_html to handle method=PUT/DELETE in forms and buttons.
import "phoenix_html"
// Establish Phoenix Socket and LiveView configuration.
import {Socket} from "phoenix"
import {LiveSocket} from "phoenix_live_view"
import {hooks as colocatedHooks} from "phoenix-colocated/fathom"
import topbar from "../vendor/topbar"
import uPlot from "../vendor/uplot/uPlot.esm.js"

// Streaming time-series chart for the admin dashboard. The LiveView renders
//   <div id="..." phx-hook="Chart" phx-update="ignore"
//        data-opts={JSON: {height, maxlen, yLabel, series: [{label, stroke, fill?, width?}]}}
//        data-initial={JSON: [[x...], [y1...], ...]}>
// then push_event(socket, "chart:<id>", %{x: t_seconds, ys: [..]}) each tick to append a point,
// or "chart:<id>:reset" with fresh data. Theme-neutral axis/grid greys read in light + dark.
const Chart = {
  mounted() {
    const opts = JSON.parse(this.el.dataset.opts || "{}")
    const maxlen = opts.maxlen || 300
    this.maxlen = maxlen
    this.data = JSON.parse(this.el.dataset.initial || "[[]]")

    const axisStroke = "#8b8b8b"
    const grid = {stroke: "rgba(125,125,125,0.18)", width: 1}
    const ticks = {stroke: "rgba(125,125,125,0.25)", width: 1}
    const axis = {stroke: axisStroke, grid, ticks, font: "11px ui-monospace, monospace"}

    const series = [{}].concat((opts.series || []).map(s => ({
      label: s.label,
      stroke: s.stroke,
      width: s.width || 1.5,
      fill: s.fill || null,
      points: {show: false},
    })))

    this.plot = new uPlot({
      width: this.el.clientWidth || 600,
      height: opts.height || 200,
      cursor: {y: false},
      legend: {show: true},
      scales: {x: {time: true}},
      axes: [axis, Object.assign({}, axis, {label: opts.yLabel})],
      series,
    }, this.data, this.el)

    this.handleEvent(`chart:${this.el.id}`, ({x, ys}) => {
      this.data[0].push(x)
      ys.forEach((y, i) => this.data[i + 1].push(y))
      if (this.data[0].length > this.maxlen) this.data.forEach(a => a.shift())
      this.plot.setData(this.data)
    })
    this.handleEvent(`chart:${this.el.id}:reset`, (fresh) => {
      this.data = fresh
      this.plot.setData(this.data)
    })

    this.ro = new ResizeObserver(() => this.plot.setSize({width: this.el.clientWidth, height: opts.height || 200}))
    this.ro.observe(this.el)
  },
  destroyed() {
    this.ro && this.ro.disconnect()
    this.plot && this.plot.destroy()
  },
}

const csrfToken = document.querySelector("meta[name='csrf-token']").getAttribute("content")
const liveSocket = new LiveSocket("/live", Socket, {
  longPollFallbackMs: 2500,
  params: {_csrf_token: csrfToken},
  hooks: {...colocatedHooks, Chart},
})

// Show progress bar on live navigation and form submits
topbar.config({barColors: {0: "#29d"}, shadowColor: "rgba(0, 0, 0, .3)"})
window.addEventListener("phx:page-loading-start", _info => topbar.show(300))
window.addEventListener("phx:page-loading-stop", _info => topbar.hide())

// connect if there are any LiveViews on the page
liveSocket.connect()

// expose liveSocket on window for web console debug logs and latency simulation:
// >> liveSocket.enableDebug()
// >> liveSocket.enableLatencySim(1000)  // enabled for duration of browser session
// >> liveSocket.disableLatencySim()
window.liveSocket = liveSocket

// The lines below enable quality of life phoenix_live_reload
// development features:
//
//     1. stream server logs to the browser console
//     2. click on elements to jump to their definitions in your code editor
//
if (process.env.NODE_ENV === "development") {
  window.addEventListener("phx:live_reload:attached", ({detail: reloader}) => {
    // Enable server log streaming to client.
    // Disable with reloader.disableServerLogs()
    reloader.enableServerLogs()

    // Open configured PLUG_EDITOR at file:line of the clicked element's HEEx component
    //
    //   * click with "c" key pressed to open at caller location
    //   * click with "d" key pressed to open at function component definition location
    let keyDown
    window.addEventListener("keydown", e => keyDown = e.key)
    window.addEventListener("keyup", _e => keyDown = null)
    window.addEventListener("click", e => {
      if(keyDown === "c"){
        e.preventDefault()
        e.stopImmediatePropagation()
        reloader.openEditorAtCaller(e.target)
      } else if(keyDown === "d"){
        e.preventDefault()
        e.stopImmediatePropagation()
        reloader.openEditorAtDef(e.target)
      }
    }, true)

    window.liveReloader = reloader
  })
}

