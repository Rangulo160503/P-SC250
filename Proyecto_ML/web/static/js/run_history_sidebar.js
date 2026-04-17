/**
 * Historial de ejecuciones: sidebar interactivo (fetch CRUD sin recargar al renombrar o eliminar).
 */
(function () {
    var root = document.getElementById("run-history-root");
    if (!root) return;

    var scrollEl = document.getElementById("run-history-scroll");
    var listEl = document.getElementById("run-history-list");
    var runsUrl = root.getAttribute("data-runs-url") || "/runs";

    function getActiveFromLocation() {
        var m = window.location.pathname.match(/^\/results\/([^/]+)\/?$/);
        if (m && m[1]) return m[1];
        return (root.getAttribute("data-active-run") || "").trim();
    }

    function fetchRuns() {
        var aid = getActiveFromLocation();
        var q = aid ? "?active=" + encodeURIComponent(aid) : "";
        return fetch(runsUrl + q, {
            method: "GET",
            headers: { Accept: "application/json" },
            credentials: "same-origin",
        }).then(function (r) {
            if (!r.ok) throw new Error("runs");
            return r.json();
        });
    }

    function scrollSnapshot() {
        return scrollEl ? scrollEl.scrollTop : 0;
    }

    function scrollRestore(y) {
        if (scrollEl) scrollEl.scrollTop = y;
    }

    function esc(s) {
        var d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function iconDots() {
        return (
            '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">' +
            '<circle cx="12" cy="5" r="1.5" fill="currentColor" stroke="none"/>' +
            '<circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none"/>' +
            '<circle cx="12" cy="19" r="1.5" fill="currentColor" stroke="none"/>' +
            "</svg>"
        );
    }

    function iconPencil() {
        return (
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">' +
            '<path d="M12 20h9M16.5 3.5a2.1 2.1 0 013 3L7 19l-4 1 1-4L16.5 3.5z"/>' +
            "</svg>"
        );
    }

    function iconTrash() {
        return (
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">' +
            '<path d="M3 6h18M8 6V4h8v2M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6M10 11v6M14 11v6"/>' +
            "</svg>"
        );
    }

    function iconOpen() {
        return (
            '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">' +
            '<path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/>' +
            "</svg>"
        );
    }

    function closeAllMenus() {
        listEl.querySelectorAll(".run-history-item.is-menu-open").forEach(function (li) {
            li.classList.remove("is-menu-open");
            var b = li.querySelector(".run-history-item__menu-btn");
            if (b) b.setAttribute("aria-expanded", "false");
        });
    }

    function closeAllConfirm() {
        listEl.querySelectorAll(".run-history-confirm").forEach(function (el) {
            el.hidden = true;
        });
    }

    function startRename(li, runId) {
        closeAllConfirm();
        closeAllMenus();
        var nameBtn = li.querySelector(".run-history-item__name");
        if (!nameBtn) return;
        var cur = nameBtn.textContent || "";
        var inp = document.createElement("input");
        inp.type = "text";
        inp.className = "run-history-item__input inline-edit";
        inp.value = cur;
        nameBtn.replaceWith(inp);
        li.classList.add("is-editing");
        inp.focus();
        inp.select();

        function refresh() {
            return fetchRuns()
                .then(renderRuns)
                .catch(function () {});
        }

        function onKey(e) {
            if (e.key === "Escape") {
                e.preventDefault();
                inp.removeEventListener("keydown", onKey);
                li.classList.remove("is-editing");
                refresh();
            }
            if (e.key === "Enter") {
                e.preventDefault();
                inp.removeEventListener("keydown", onKey);
                var v = inp.value.trim();
                li.classList.remove("is-editing");
                if (!v) {
                    refresh();
                    return;
                }
                fetch("/runs/" + encodeURIComponent(runId), {
                    method: "PUT",
                    headers: {
                        "Content-Type": "application/json",
                        Accept: "application/json",
                    },
                    credentials: "same-origin",
                    body: JSON.stringify({ nombre: v }),
                })
                    .then(function (r) {
                        if (!r.ok) throw new Error();
                        return r.json();
                    })
                    .then(refresh)
                    .catch(refresh);
            }
        }
        inp.addEventListener("keydown", onKey);
    }

    function showDeleteConfirm(li) {
        closeAllMenus();
        closeAllConfirm();
        var c = li.querySelector(".run-history-confirm");
        if (c) c.hidden = false;
    }

    function doDelete(runId) {
        fetch("/runs/" + encodeURIComponent(runId), {
            method: "DELETE",
            headers: { Accept: "application/json" },
            credentials: "same-origin",
        })
            .then(function (r) {
                if (!r.ok) throw new Error();
                return r.json();
            })
            .then(function () {
                var path = window.location.pathname || "";
                var m = path.match(/^\/results\/([^/]+)\/?$/);
                if (m && m[1] === runId) {
                    window.location.href = "/results";
                    return null;
                }
                return fetchRuns();
            })
            .then(function (data) {
                if (data) renderRuns(data);
            })
            .catch(function () {});
    }

    function renderRuns(data) {
        var runs = data.runs || [];
        var y = scrollSnapshot();
        listEl.innerHTML = "";
        if (!runs.length) {
            var empty = document.createElement("li");
            empty.className = "run-history-empty";
            empty.textContent = "Sin ejecuciones guardadas.";
            listEl.appendChild(empty);
            scrollRestore(y);
            return;
        }
        runs.forEach(function (run) {
            var li = document.createElement("li");
            li.className = "run-history-item";
            li.setAttribute("data-run-id", run.id);
            if (run.is_active) li.classList.add("is-active");

            var row = document.createElement("div");
            row.className = "run-history-item__row";

            var main = document.createElement("div");
            main.className = "run-history-item__main";

            var nameBtn = document.createElement("button");
            nameBtn.type = "button";
            nameBtn.className = "run-history-item__name";
            nameBtn.textContent = run.nombre || "Ejecución";

            var meta = document.createElement("div");
            meta.className = "run-history-item__meta";
            meta.innerHTML =
                "<span class=\"run-history-item__fecha\">" +
                esc(run.fecha) +
                "</span>" +
                "<span class=\"run-history-item__dataset\">" +
                esc(run.dataset) +
                "</span>";

            main.appendChild(nameBtn);
            main.appendChild(meta);

            var actions = document.createElement("div");
            actions.className = "run-history-item__actions";
            var menuBtn = document.createElement("button");
            menuBtn.type = "button";
            menuBtn.className = "run-history-item__menu-btn";
            menuBtn.setAttribute("aria-expanded", "false");
            menuBtn.setAttribute("aria-haspopup", "true");
            menuBtn.setAttribute("aria-label", "Más acciones");
            menuBtn.innerHTML = iconDots();

            var menu = document.createElement("div");
            menu.className = "run-history-menu";
            menu.setAttribute("role", "menu");
            menu.innerHTML =
                "<button type=\"button\" class=\"run-history-menu__i\" data-action=\"rename\" role=\"menuitem\">" +
                iconPencil() +
                "<span>Renombrar</span></button>" +
                "<button type=\"button\" class=\"run-history-menu__i\" data-action=\"delete\" role=\"menuitem\">" +
                iconTrash() +
                "<span>Eliminar</span></button>" +
                "<button type=\"button\" class=\"run-history-menu__i\" data-action=\"open\" role=\"menuitem\">" +
                iconOpen() +
                "<span>Abrir</span></button>";

            actions.appendChild(menuBtn);
            actions.appendChild(menu);

            var confirm = document.createElement("div");
            confirm.className = "run-history-confirm";
            confirm.hidden = true;
            confirm.innerHTML =
                "<p class=\"run-history-confirm__t\">¿Eliminar ejecución?</p>" +
                "<div class=\"run-history-confirm__a\">" +
                "<button type=\"button\" class=\"run-history-btn run-history-btn--danger\" data-action=\"confirm-delete\">Sí</button>" +
                "<button type=\"button\" class=\"run-history-btn run-history-btn--ghost\" data-action=\"cancel-delete\">Cancelar</button>" +
                "</div>";

            row.appendChild(main);
            row.appendChild(actions);
            li.appendChild(row);
            li.appendChild(confirm);
            listEl.appendChild(li);

            nameBtn.addEventListener("click", function () {
                if (run.url) window.location.href = run.url;
            });

            menuBtn.addEventListener("click", function (e) {
                e.stopPropagation();
                var open = li.classList.toggle("is-menu-open");
                menuBtn.setAttribute("aria-expanded", open ? "true" : "false");
                if (open) {
                    listEl.querySelectorAll(".run-history-item.is-menu-open").forEach(function (x) {
                        if (x !== li) {
                            x.classList.remove("is-menu-open");
                            var mb = x.querySelector(".run-history-item__menu-btn");
                            if (mb) mb.setAttribute("aria-expanded", "false");
                        }
                    });
                }
            });

            menu.addEventListener("click", function (e) {
                var t = e.target.closest("[data-action]");
                if (!t) return;
                e.stopPropagation();
                var act = t.getAttribute("data-action");
                li.classList.remove("is-menu-open");
                menuBtn.setAttribute("aria-expanded", "false");
                if (act === "open" && run.url) window.location.href = run.url;
                if (act === "rename") startRename(li, run.id);
                if (act === "delete") showDeleteConfirm(li);
            });

            confirm.querySelector("[data-action=\"confirm-delete\"]").addEventListener("click", function () {
                confirm.hidden = true;
                doDelete(run.id);
            });
            confirm.querySelector("[data-action=\"cancel-delete\"]").addEventListener("click", function () {
                confirm.hidden = true;
            });
        });
        scrollRestore(y);
    }

    document.addEventListener("click", function () {
        closeAllMenus();
    });

    var clearBtn = document.getElementById("run-history-clear-btn");
    var clearFeedback = document.getElementById("run-history-clear-feedback");
    var clearUrl =
        (clearBtn && clearBtn.getAttribute("data-clear-url")) || "/api/history/clear";

    if (clearBtn) {
        clearBtn.addEventListener("click", function () {
            if (
                !window.confirm(
                    "¿Seguro que deseas eliminar todo el historial?"
                )
            ) {
                return;
            }
            clearBtn.disabled = true;
            clearBtn.setAttribute("aria-busy", "true");
            if (clearFeedback) {
                clearFeedback.hidden = false;
                clearFeedback.textContent = "Limpiando…";
            }
            fetch(clearUrl, {
                method: "POST",
                headers: {
                    Accept: "application/json",
                    "Content-Type": "application/json",
                },
                credentials: "same-origin",
                body: "{}",
            })
                .then(function (r) {
                    return r.json().catch(function () {
                        return { ok: false, error: "No se pudo leer la respuesta del servidor." };
                    }).then(function (data) {
                        return { okHttp: r.ok, data: data };
                    });
                })
                .then(function (res) {
                    var d = res.data || {};
                    if (res.okHttp && d.ok) {
                        if (clearFeedback) {
                            clearFeedback.textContent =
                                d.message || "Historial limpiado correctamente.";
                        }
                        return fetchRuns().then(renderRuns).then(function () {
                            var m = window.location.pathname.match(
                                /^\/results\/([^/]+)\/?$/
                            );
                            if (m && m[1]) {
                                window.location.href = "/results";
                                return;
                            }
                            if (clearFeedback) {
                                window.setTimeout(function () {
                                    clearFeedback.textContent = "";
                                    clearFeedback.hidden = true;
                                }, 2000);
                            }
                        });
                    }
                    var msg =
                        d.error ||
                        d.message ||
                        "No se pudo limpiar el historial.";
                    console.error(msg);
                    window.alert(msg);
                    if (clearFeedback) {
                        clearFeedback.textContent = "";
                        clearFeedback.hidden = true;
                    }
                })
                .catch(function (err) {
                    console.error(err);
                    window.alert("Error de red al limpiar el historial.");
                    if (clearFeedback) {
                        clearFeedback.textContent = "";
                        clearFeedback.hidden = true;
                    }
                })
                .finally(function () {
                    clearBtn.disabled = false;
                    clearBtn.removeAttribute("aria-busy");
                });
        });
    }

    fetchRuns()
        .then(renderRuns)
        .catch(function () {
            listEl.innerHTML =
                "<li class=\"run-history-empty\">No se pudo cargar el historial.</li>";
        });
})();
