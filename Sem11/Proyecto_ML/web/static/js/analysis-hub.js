/**
 * Hubs (Explorar análisis / Ver visualizaciones): diálogo de selección + panel lateral
 * que carga HTML vía /partial/<slug> sin navegación completa.
 */
(function () {
    function wire(idDialog, idOpen) {
        var dlg = document.getElementById(idDialog);
        var openBtn = document.getElementById(idOpen);
        if (!dlg || !openBtn) return;
        var closes = dlg.querySelectorAll("[data-hub-close]");
        openBtn.addEventListener("click", function () {
            if (typeof dlg.showModal === "function") {
                dlg.showModal();
            } else {
                dlg.setAttribute("open", "");
            }
        });
        closes.forEach(function (btn) {
            btn.addEventListener("click", function () {
                if (typeof dlg.close === "function") dlg.close();
                else dlg.removeAttribute("open");
            });
        });
        dlg.addEventListener("click", function (ev) {
            if (ev.target === dlg) {
                if (typeof dlg.close === "function") dlg.close();
                else dlg.removeAttribute("open");
            }
        });
    }

    wire("analysis-hub-dialog", "open-analysis-hub");
    wire("viz-hub-dialog", "open-viz-hub");

    document.querySelectorAll(".js-open-analysis-hub").forEach(function (b) {
        b.addEventListener("click", function () {
            var t = document.getElementById("open-analysis-hub");
            if (t) t.click();
        });
    });

    var backdrop = document.getElementById("overlay-backdrop");
    var panel = document.getElementById("analysis-panel");
    var content = document.getElementById("panel-content");
    var closeBtn = document.getElementById("close-analysis-panel");
    var panelOpen = false;

    function closeHubDialogFrom(el) {
        var dlg = el && el.closest("dialog");
        if (dlg && typeof dlg.close === "function") {
            dlg.close();
        }
    }

    function runScripts(root) {
        if (!root) return;
        root.querySelectorAll("script").forEach(function (old) {
            var s = document.createElement("script");
            for (var i = 0; i < old.attributes.length; i++) {
                var a = old.attributes[i];
                if (a.name === "src") {
                    s.src = a.value;
                } else {
                    s.setAttribute(a.name, a.value);
                }
            }
            if (!old.src) {
                s.textContent = old.textContent;
            }
            old.parentNode.replaceChild(s, old);
        });
    }

    function closePanel() {
        panelOpen = false;
        if (backdrop) {
            backdrop.classList.remove("is-open");
            backdrop.setAttribute("aria-hidden", "true");
        }
        if (panel) {
            panel.classList.remove("is-open");
            panel.setAttribute("aria-hidden", "true");
        }
        document.body.classList.remove("is-analysis-panel-open");
        if (content) content.innerHTML = "";
    }

    function openPanel(module, originEl) {
        if (!module || !content || !backdrop || !panel) return;
        if (originEl) closeHubDialogFrom(originEl);
        panelOpen = true;
        backdrop.classList.add("is-open");
        backdrop.setAttribute("aria-hidden", "false");
        panel.classList.add("is-open");
        panel.setAttribute("aria-hidden", "false");
        document.body.classList.add("is-analysis-panel-open");
        content.innerHTML = '<p class="ms-muted mb-0">Cargando…</p>';

        fetch("/partial/" + encodeURIComponent(module), { credentials: "same-origin" })
            .then(function (r) {
                if (!r.ok) throw new Error("bad status");
                return r.text();
            })
            .then(function (html) {
                if (!panelOpen) return;
                content.innerHTML = html;
                runScripts(content);
                if (closeBtn) {
                    try {
                        closeBtn.focus();
                    } catch (e) {}
                }
            })
            .catch(function () {
                if (!panelOpen) return;
                content.innerHTML =
                    '<p class="ms-muted mb-0">No se pudo cargar el módulo. Intentá de nuevo o abrí la misma sección desde la barra superior.</p>';
            });
    }

    document.addEventListener("click", function (ev) {
        var card = ev.target.closest(".analysis-card, .viz-card");
        if (!card || !card.hasAttribute("data-module")) return;
        ev.preventDefault();
        ev.stopPropagation();
        var mod = card.getAttribute("data-module");
        openPanel(mod, card);
    });

    document.addEventListener("keydown", function (ev) {
        if (panelOpen && ev.key === "Escape") {
            closePanel();
            return;
        }
        var card = ev.target.closest(".analysis-card, .viz-card");
        if (!card || !card.hasAttribute("data-module")) return;
        if (ev.key !== "Enter" && ev.key !== " ") return;
        ev.preventDefault();
        card.click();
    });

    if (closeBtn) closeBtn.addEventListener("click", closePanel);
    if (backdrop) backdrop.addEventListener("click", closePanel);
})();
