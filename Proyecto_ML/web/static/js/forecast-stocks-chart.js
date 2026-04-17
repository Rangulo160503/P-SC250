/**
 * Gráfico histórico + pronóstico (Chart.js): horizontes 3m y 60m, zoom/pan, rangos.
 */
(function () {
    "use strict";

    function parseCsvNumericSeries(text) {
        var out = [];
        var lines = String(text || "").split(/\r?\n/);
        for (var i = 0; i < lines.length; i++) {
            var line = lines[i].trim();
            if (!line) continue;
            var parts = line.split(",");
            if (parts.length < 2) continue;
            var dRaw = parts[0].trim();
            var vRaw = parts.slice(1).join(",").trim();
            if (/^fecha$/i.test(dRaw)) continue;
            if (!/^\d{4}-\d{2}-\d{2}/.test(dRaw)) continue;
            var d = dRaw.slice(0, 10);
            var v = parseFloat(vRaw.replace(/\s/g, "").replace(",", "."));
            if (isNaN(v)) continue;
            out.push({ d: d, v: v });
        }
        return out;
    }

    function sortByDate(rows) {
        return rows.slice().sort(function (a, b) {
            return a.d.localeCompare(b.d);
        });
    }

    function dedupeLastPerDate(rows) {
        var map = {};
        for (var i = 0; i < rows.length; i++) {
            map[rows[i].d] = rows[i].v;
        }
        var keys = Object.keys(map).sort();
        return keys.map(function (k) {
            return { d: k, v: map[k] };
        });
    }

    function mergeHistForecast(histRows, fcRows) {
        if (!fcRows.length) {
            return { labels: [], hist: [], fc: [] };
        }
        var fc = sortByDate(dedupeLastPerDate(fcRows));
        var hist = sortByDate(dedupeLastPerDate(histRows));
        var firstFc = fc[0].d;
        var histFiltered = [];
        for (var i = 0; i < hist.length; i++) {
            if (hist[i].d < firstFc) histFiltered.push(hist[i]);
        }
        var labels = [];
        var histData = [];
        var fcData = [];
        for (var h = 0; h < histFiltered.length; h++) {
            labels.push(histFiltered[h].d);
            histData.push(histFiltered[h].v);
            fcData.push(null);
        }
        for (var f = 0; f < fc.length; f++) {
            labels.push(fc[f].d);
            histData.push(null);
            fcData.push(fc[f].v);
        }
        return { labels: labels, hist: histData, fc: fcData };
    }

    function lastNonNull(arr) {
        for (var i = arr.length - 1; i >= 0; i--) {
            if (arr[i] != null && !isNaN(arr[i])) return arr[i];
        }
        return null;
    }

    function fmtNumber(n) {
        if (n == null || isNaN(n)) return "—";
        try {
            return Math.round(Number(n)).toLocaleString("es-EC");
        } catch (e) {
            return String(n);
        }
    }

    function fmtLabel(iso) {
        if (!iso) return "—";
        try {
            var p = iso.split("-");
            if (p.length >= 2) {
                var mo = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"];
                var m = parseInt(p[1], 10) - 1;
                if (m >= 0 && m < 12) return mo[m] + " " + p[0];
            }
            return iso;
        } catch (e2) {
            return iso;
        }
    }

    function fetchText(url) {
        return fetch(url, { credentials: "same-origin" }).then(function (r) {
            if (!r.ok) throw new Error(String(r.status));
            return r.text();
        });
    }

    function tryForecastUrls(urls) {
        if (!urls || !urls.length) return Promise.reject(new Error("no_forecast_csv"));
        var i = 0;
        function next() {
            if (i >= urls.length) return Promise.reject(new Error("no_forecast_csv"));
            var u = urls[i++];
            if (!u) return next();
            return fetchText(u)
                .then(function (t) {
                    var rows = parseCsvNumericSeries(t);
                    if (!rows.length) return next();
                    return rows;
                })
                .catch(function () {
                    return next();
                });
        }
        return next();
    }

    function tryHistoryUrls(urls) {
        if (!urls || !urls.length) return Promise.resolve([]);
        var j = 0;
        function nextH() {
            if (j >= urls.length) return Promise.resolve([]);
            var u = urls[j++];
            return fetchText(u)
                .then(function (t) {
                    var rows = parseCsvNumericSeries(t);
                    if (rows.length) return rows;
                    return nextH();
                })
                .catch(function () {
                    return nextH();
                });
        }
        return nextH();
    }

    function zoomPluginReady() {
        try {
            return (
                typeof Chart !== "undefined" &&
                Chart.registry &&
                Chart.registry.getPlugin &&
                !!Chart.registry.getPlugin("zoom")
            );
        } catch (e) {
            return false;
        }
    }

    function chartColors() {
        var dark = document.documentElement.getAttribute("data-theme") === "dark";
        return {
            hist: dark ? "#4cc2ff" : "#0b6cbd",
            fc: dark ? "#7ee787" : "#2e7d32",
            grid: dark ? "rgba(255,255,255,0.08)" : "rgba(0,0,0,0.06)",
            text: dark ? "#c9d1d9" : "#5c5c5c",
        };
    }

    function uniqueUrls(arr) {
        var out = [];
        (arr || []).forEach(function (u) {
            if (u && out.indexOf(u) < 0) out.push(u);
        });
        return out;
    }

    function boot() {
        var cfgEl = document.getElementById("fc-chart-config");
        var canvas = document.getElementById("fc-stocks-canvas");
        if (!cfgEl || !canvas || typeof Chart === "undefined") return;

        var cfg;
        try {
            cfg = JSON.parse(cfgEl.textContent || "{}");
        } catch (e) {
            return;
        }
        console.log("[forecast] Forecast config:", cfg);

        var urls3m = uniqueUrls([cfg.forecast_3m_url, cfg.forecast_3m_int_url].concat(cfg.forecastUrls || []));
        var urls60m = uniqueUrls([cfg.forecast_60m_url, cfg.forecast_60m_int_url]);
        var historyUrls = cfg.historyUrls || [];
        if (!urls3m.length) return;

        var zoomPluginOk = zoomPluginReady();
        var valueEl = document.getElementById("fc-chart-value");
        var dateEl = document.getElementById("fc-chart-date");
        var hintEl = document.getElementById("fc-chart-hint");
        var insightEl = document.getElementById("fc-insight-box");
        var horizonStatusEl = document.getElementById("fc-horizon-status");
        var rangeBtns = document.querySelectorAll(".fc-range-selector button[data-range]");
        var horizonBtns = document.querySelectorAll(".forecast-horizon-selector button[data-horizon]");
        var resetZoomBtn = document.getElementById("fc-reset-zoom");

        var histRows = [];
        var rowsFc3m = [];
        var rowsFc60m = [];
        var currentHorizon = "3m";
        var currentRange = "all";
        var full = { labels: [], hist: [], fc: [] };
        var chart = null;

        function update60mHorizonButton() {
            var b = document.getElementById("fc-horizon-60m");
            if (!b) return;
            var ok = rowsFc60m && rowsFc60m.length > 0;
            if (ok) {
                b.disabled = false;
                b.setAttribute("aria-disabled", "false");
                b.removeAttribute("title");
            } else {
                b.disabled = true;
                b.setAttribute("aria-disabled", "true");
                if (cfg.hasForecast60mFile) {
                    b.title =
                        "El servidor detectó forecast_60m pero no se pudieron leer filas (CSV vacío, error de red o permisos). Recargue o regenere artifacts.";
                } else {
                    b.title =
                        "No hay datos de 5 años para este run. Ejecute ML1 (versión con forecast_60m) o elija el run más reciente en el historial.";
                }
            }
        }

        function setHorizonStatus() {
            if (!horizonStatusEl) return;
            horizonStatusEl.textContent =
                currentHorizon === "60m"
                    ? "Horizonte seleccionado: 5 años (60 meses)"
                    : "Horizonte seleccionado: 3 meses";
        }

        function setActiveHorizonBtn(btn) {
            horizonBtns.forEach(function (b) {
                b.classList.toggle("is-active", b === btn);
            });
        }

        function rebuildFull() {
            var fcRows = currentHorizon === "60m" ? rowsFc60m : rowsFc3m;
            full = mergeHistForecast(histRows, fcRows);
        }

        function setInsight() {
            if (!insightEl) return;
            var lastReal = lastNonNull(full.hist);
            var lastFc = lastNonNull(full.fc);
            var msg = "";
            if (lastReal == null && lastFc != null) {
                msg = "Solo horizonte de pronóstico visible (sin serie histórica en artifacts).";
            } else if (lastReal != null && lastFc != null) {
                if (lastFc > lastReal) {
                    msg =
                        "Tendencia creciente hacia el final del horizonte mostrado (último pronóstico por encima del último dato real).";
                } else if (lastFc < lastReal) {
                    msg =
                        "Tendencia decreciente hacia el final del horizonte mostrado (último pronóstico por debajo del último dato real).";
                } else {
                    msg = "Comportamiento estable esperado al cierre del horizonte visible.";
                }
            } else {
                msg = "—";
            }
            if (currentHorizon === "60m") {
                msg += " Nota: a 5 años la incertidumbre del modelo suele ser mayor.";
            }
            insightEl.textContent = msg;
        }

        function setHeaderFromChartIndex(idx) {
            if (!chart || !valueEl || !dateEl) return;
            if (idx < 0 || idx >= chart.data.labels.length) {
                valueEl.textContent = "—";
                dateEl.textContent = "Pase el cursor sobre el gráfico";
                return;
            }
            var lab = chart.data.labels[idx];
            var vh = chart.data.datasets[0].data[idx];
            var vf = chart.data.datasets[1].data[idx];
            var show = vf != null ? vf : vh;
            valueEl.textContent = fmtNumber(show);
            dateEl.textContent = fmtLabel(lab);
            if (hintEl) {
                if (vf != null && vh == null) hintEl.textContent = "Pronóstico";
                else if (vh != null) hintEl.textContent = "Histórico";
                else hintEl.textContent = "";
            }
        }

        function setHeaderLastInView() {
            if (!chart || !chart.data.labels.length) return;
            setHeaderFromChartIndex(chart.data.labels.length - 1);
        }

        function sliceRange(rangeKey) {
            var n = full.labels.length;
            if (!n) return { labels: [], hist: [], fc: [] };
            var take;
            if (rangeKey === "all" || !rangeKey) take = n;
            else take = Math.min(parseInt(rangeKey, 10) || n, n);
            var start = Math.max(0, n - take);
            return {
                labels: full.labels.slice(start),
                hist: full.hist.slice(start),
                fc: full.fc.slice(start),
            };
        }

        function applyRangeToChart(rangeKey) {
            if (!chart) return;
            var sl = sliceRange(rangeKey);
            chart.data.labels = sl.labels;
            chart.data.datasets[0].data = sl.hist;
            chart.data.datasets[1].data = sl.fc;
            if (chart.resetZoom) chart.resetZoom();
            chart.update();
            setHeaderLastInView();
        }

        function setActiveRangeBtn(btn) {
            rangeBtns.forEach(function (b) {
                b.classList.toggle("is-active", b === btn);
            });
        }

        function maxTicksForHorizon() {
            return currentHorizon === "60m" ? 14 : 8;
        }

        function wireChartAfterCreate() {
            chart.canvas.addEventListener("mousemove", function (event) {
                var pts = chart.getElementsAtEventForMode(
                    event,
                    "index",
                    { intersect: false },
                    false
                );
                if (pts.length) setHeaderFromChartIndex(pts[0].index);
            });
            chart.canvas.addEventListener("mouseleave", function () {
                setHeaderLastInView();
            });

            rangeBtns.forEach(function (btn) {
                btn.addEventListener("click", function () {
                    var r = btn.getAttribute("data-range") || "all";
                    currentRange = r;
                    setActiveRangeBtn(btn);
                    applyRangeToChart(r);
                });
            });

            if (resetZoomBtn) {
                resetZoomBtn.addEventListener("click", function () {
                    if (chart && chart.resetZoom) chart.resetZoom();
                });
            }

            horizonBtns.forEach(function (btn) {
                btn.addEventListener("click", function () {
                    if (btn.disabled) return;
                    var h = btn.getAttribute("data-horizon") || "3m";
                    if (h === "60m" && !rowsFc60m.length) return;
                    currentHorizon = h === "60m" ? "60m" : "3m";
                    setActiveHorizonBtn(btn);
                    setHorizonStatus();
                    rebuildFull();
                    if (!full.labels.length) {
                        if (hintEl) hintEl.textContent = "No hay datos para este horizonte.";
                        return;
                    }
                    try {
                        chart.options.scales.x.ticks.maxTicksLimit = maxTicksForHorizon();
                    } catch (e1) {
                        /* ignore */
                    }
                    setInsight();
                    applyRangeToChart(currentRange);
                });
            });

            window.addEventListener("resize", function () {
                if (chart) chart.resize();
            });

            var themeMo = new MutationObserver(function () {
                if (!chart) return;
                var c2 = chartColors();
                chart.options.scales.x.ticks.color = c2.text;
                chart.options.scales.y.ticks.color = c2.text;
                chart.options.scales.x.grid.color = c2.grid;
                chart.options.scales.y.grid.color = c2.grid;
                chart.data.datasets[0].borderColor = c2.hist;
                chart.data.datasets[1].borderColor = c2.fc;
                chart.update("none");
            });
            themeMo.observe(document.documentElement, {
                attributes: true,
                attributeFilter: ["data-theme"],
            });

            var allBtn = document.querySelector('.fc-range-selector button[data-range="all"]');
            if (allBtn) setActiveRangeBtn(allBtn);
            setHorizonStatus();
            setInsight();
            applyRangeToChart("all");
        }

        Promise.all([
            tryHistoryUrls(historyUrls),
            tryForecastUrls(urls3m),
            tryForecastUrls(urls60m).catch(function () {
                return [];
            }),
        ])
            .then(function (triple) {
                histRows = triple[0] || [];
                rowsFc3m = triple[1] || [];
                rowsFc60m = triple[2] || [];
                console.log(
                    "[forecast] 60m rows:",
                    rowsFc60m ? rowsFc60m.length : 0,
                    "hasForecast60mFile:",
                    cfg.hasForecast60mFile,
                    "forecast_60m_url:",
                    cfg.forecast_60m_url
                );

                update60mHorizonButton();

                currentHorizon = "3m";
                rebuildFull();
                if (!full.labels.length) {
                    if (hintEl) hintEl.textContent = "No se pudieron leer los datos del pronóstico.";
                    return;
                }

                var colors = chartColors();
                var ctx = canvas.getContext("2d");
                if (!ctx) return;

                var pluginsBlock = {
                    legend: { display: false },
                    tooltip: {
                        enabled: true,
                        mode: "index",
                        intersect: false,
                    },
                };
                if (zoomPluginOk) {
                    pluginsBlock.zoom = {
                        pan: { enabled: true, mode: "x" },
                        zoom: {
                            wheel: { enabled: true },
                            pinch: { enabled: true },
                            mode: "x",
                        },
                    };
                }

                chart = new Chart(ctx, {
                    type: "line",
                    data: {
                        labels: full.labels.slice(),
                        datasets: [
                            {
                                label: "Histórico",
                                data: full.hist.slice(),
                                borderColor: colors.hist,
                                backgroundColor: "transparent",
                                tension: 0.3,
                                spanGaps: false,
                                pointRadius: 0,
                                pointHoverRadius: 4,
                                borderWidth: 2,
                            },
                            {
                                label: "Pronóstico",
                                data: full.fc.slice(),
                                borderColor: colors.fc,
                                backgroundColor: "transparent",
                                borderDash: [6, 6],
                                tension: 0.3,
                                spanGaps: false,
                                pointRadius: 0,
                                pointHoverRadius: 4,
                                borderWidth: 2,
                            },
                        ],
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        animation: {
                            duration: 1200,
                            easing: "easeOutQuart",
                        },
                        interaction: { mode: "index", intersect: false },
                        plugins: pluginsBlock,
                        scales: {
                            x: {
                                ticks: {
                                    maxRotation: 0,
                                    autoSkip: true,
                                    maxTicksLimit: maxTicksForHorizon(),
                                    color: colors.text,
                                },
                                grid: { color: colors.grid },
                            },
                            y: {
                                ticks: { color: colors.text },
                                grid: { color: colors.grid },
                            },
                        },
                    },
                });

                wireChartAfterCreate();
            })
            .catch(function () {
                if (hintEl) hintEl.textContent = "No se pudieron cargar los CSV de pronóstico.";
            });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
