/**
 * Carga de datos: listar, subir, renombrar, eliminar (sin selección manual de “activo”).
 */
(function () {
    var mount = document.getElementById("dataset-app-mount");
    if (!mount) return;

    var listUrl = mount.getAttribute("data-list-url") || "/api/datasets";
    var renameUrl = mount.getAttribute("data-rename-url") || "/api/datasets/rename";
    var deleteUrl = mount.getAttribute("data-delete-url") || "/api/datasets/delete";
    var uploadUrl = mount.getAttribute("data-upload-url") || "/api/datasets/upload";
    var runUrl = mount.getAttribute("data-run-url") || "/run";

    var listRoot = document.getElementById("dataset-list-root");
    var fileInput = document.getElementById("dataset-file-input");
    var uploadBtn = document.getElementById("dataset-upload-btn");
    var fileList = document.getElementById("file-list");
    var progressWrap = document.getElementById("upload-progress");
    var progressFill = document.getElementById("upload-progress-fill");
    var progressText = document.getElementById("upload-progress-text");
    var goToProcess = document.getElementById("go-to-process");

    var selected = (mount.getAttribute("data-active-initial") || "").trim();
    var openMenu = null;

    function esc(s) {
        var d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function closeMenus() {
        if (openMenu) {
            openMenu.classList.remove("is-open");
            var wrap = openMenu.closest(".dataset-card-menu-wrap");
            var card = openMenu.closest(".dataset-card");
            var kb = wrap && wrap.querySelector(".dataset-card-kebab");
            if (kb) kb.setAttribute("aria-expanded", "false");
            if (card) card.classList.remove("is-menu-open");
            openMenu = null;
        }
    }

    document.addEventListener("click", function () {
        closeMenus();
    });

    function jsonHeaders() {
        return { "Content-Type": "application/json", Accept: "application/json" };
    }

    function updateProcessGate(datasets) {
        var n = datasets && datasets.length ? datasets.length : 0;
        if (goToProcess) {
            goToProcess.disabled = n === 0;
        }
    }

    function renderList(datasets) {
        if (!listRoot) return;
        if (!datasets || !datasets.length) {
            listRoot.innerHTML =
                '<p class="dataset-grid-empty text-secondary">No hay CSV en la carpeta de datos. Sube un archivo.</p>';
            listRoot.setAttribute("role", "status");
            updateProcessGate([]);
            return;
        }
        listRoot.setAttribute("role", "list");
        var html = "";
        datasets.forEach(function (ds) {
            var n = ds.name;
            var safeAttr = String(n).replace(/&/g, "&amp;").replace(/"/g, "&quot;");
            html +=
                '<div class="dataset-item dataset-card dataset-card--file" data-name="' +
                safeAttr +
                '" role="listitem">' +
                '<div class="dataset-card-menu-wrap">' +
                '<button type="button" class="dataset-card-kebab" aria-haspopup="true" aria-expanded="false" title="Más acciones">...</button>' +
                '<div class="dataset-card-menu" role="menu">' +
                '<button type="button" class="dataset-card-menu-item" data-action="rename">Renombrar</button>' +
                '<button type="button" class="dataset-card-menu-item dataset-card-menu-item--danger" data-action="delete">Eliminar</button>' +
                "</div></div>" +
                '<span class="dataset-item__name">' +
                esc(n) +
                "</span>" +
                "</div>";
        });
        listRoot.innerHTML = html;

        listRoot.querySelectorAll(".dataset-card-kebab").forEach(function (kb) {
            kb.addEventListener("click", function (e) {
                e.stopPropagation();
                var wrap = kb.closest(".dataset-card-menu-wrap");
                var menu = wrap && wrap.querySelector(".dataset-card-menu");
                var card = kb.closest(".dataset-card");
                if (!menu || !card) return;
                var wasOpen = menu.classList.contains("is-open");
                closeMenus();
                if (!wasOpen) {
                    menu.classList.add("is-open");
                    openMenu = menu;
                    card.classList.add("is-menu-open");
                    kb.setAttribute("aria-expanded", "true");
                }
            });
        });

        listRoot.querySelectorAll(".dataset-card-menu-item").forEach(function (item) {
            item.addEventListener("click", function (e) {
                e.stopPropagation();
                var card = item.closest(".dataset-card");
                if (!card) return;
                var name = card.getAttribute("data-name");
                var action = item.getAttribute("data-action");
                closeMenus();
                if (action === "delete") {
                    if (!confirm("¿Eliminar " + name + "?")) return;
                    fetch(deleteUrl, {
                        method: "POST",
                        credentials: "same-origin",
                        headers: jsonHeaders(),
                        body: JSON.stringify({ name: name }),
                    })
                        .then(function (r) {
                            return r.json();
                        })
                        .then(function (data) {
                            if (!data.ok) {
                                alert(data.error || "No se pudo eliminar.");
                                return;
                            }
                            if (selected === name) {
                                selected = "";
                            }
                            refreshList();
                        });
                } else if (action === "rename") {
                    var input = document.createElement("input");
                    input.type = "text";
                    input.className = "inline-edit";
                    input.value = name;
                    var row = card.querySelector(".dataset-item__name");
                    if (!row) return;
                    var oldText = row.textContent;
                    row.replaceWith(input);
                    input.focus();
                    input.select();
                    var renameDone = false;
                    var renameSubmitting = false;

                    function finish(ok, newName) {
                        if (renameDone) return;
                        renameDone = true;
                        renameSubmitting = false;
                        if (!ok) {
                            var span = document.createElement("span");
                            span.className = "dataset-item__name";
                            span.textContent = oldText;
                            if (input.parentNode) input.replaceWith(span);
                            return;
                        }
                        selected = newName;
                        refreshList();
                    }

                    function save() {
                        if (renameDone || renameSubmitting) return;
                        var to = input.value.trim();
                        if (!to || to === name) {
                            finish(false);
                            return;
                        }
                        renameSubmitting = true;
                        fetch(renameUrl, {
                            method: "POST",
                            credentials: "same-origin",
                            headers: jsonHeaders(),
                            body: JSON.stringify({ from: name, to: to }),
                        })
                            .then(function (r) {
                                return r.json();
                            })
                            .then(function (data) {
                                renameSubmitting = false;
                                if (!data.ok) {
                                    alert(data.error || "No se pudo renombrar.");
                                    finish(false);
                                    return;
                                }
                                finish(true, data.name || to);
                            })
                            .catch(function () {
                                renameSubmitting = false;
                                alert("Error de red.");
                                finish(false);
                            });
                    }

                    input.addEventListener("keydown", function (ev) {
                        if (ev.key === "Enter") {
                            ev.preventDefault();
                            save();
                        } else if (ev.key === "Escape") {
                            ev.preventDefault();
                            finish(false);
                        }
                    });
                    input.addEventListener("blur", function () {
                        window.setTimeout(function () {
                            if (renameDone || !document.body.contains(input)) return;
                            save();
                        }, 150);
                    });
                }
            });
        });

        updateProcessGate(datasets);
    }

    function refreshList() {
        fetch(listUrl, { credentials: "same-origin" })
            .then(function (r) {
                return r.json();
            })
            .then(function (data) {
                if (!data.ok || !data.datasets) {
                    renderList([]);
                    return;
                }
                var names = {};
                data.datasets.forEach(function (d) {
                    names[d.name] = true;
                });
                if (selected && !names[selected]) {
                    selected = "";
                }
                renderList(data.datasets);
            })
            .catch(function () {
                renderList([]);
            });
    }

    function renderSelectedFiles() {
        if (!fileList || !fileInput) return;
        fileList.innerHTML = "";
        if (!fileInput.files || !fileInput.files.length) return;
        Array.prototype.forEach.call(fileInput.files, function (file) {
            var li = document.createElement("li");
            li.textContent = file.name;
            fileList.appendChild(li);
        });
    }

    if (fileInput) {
        fileInput.addEventListener("change", function () {
            renderSelectedFiles();
        });
    }

    function updateProgress(done, total, filename) {
        if (!progressWrap || !progressFill || !progressText) return;
        progressWrap.classList.remove("hidden");
        var percent = total > 0 ? Math.round((done / total) * 100) : 0;
        progressFill.style.width = percent + "%";
        progressText.textContent = "Subidos " + done + "/" + total + ": " + filename;
    }

    function setUploadInFlight(index, total, filename) {
        if (!progressWrap || !progressFill || !progressText) return;
        progressWrap.classList.remove("hidden");
        var percent = total > 0 ? Math.round((index / total) * 100) : 0;
        progressFill.style.width = percent + "%";
        progressText.textContent =
            "Enviando archivo " + (index + 1) + "/" + total + ": " + filename;
    }

    function showUploadComplete(done, total) {
        if (!progressWrap || !progressFill || !progressText) return;
        progressWrap.classList.remove("hidden");
        var percent = total > 0 ? Math.round((done / total) * 100) : 100;
        progressFill.style.width = percent + "%";
        progressText.textContent = "Completado: " + done + "/" + total + " archivos subidos correctamente.";
    }

    function showUploadError(name, err) {
        console.warn("Error en " + name + ":", err);
    }

    function disableUploadUI() {
        if (uploadBtn) uploadBtn.disabled = true;
        if (fileInput) fileInput.disabled = true;
    }

    function enableUploadUI() {
        if (uploadBtn) uploadBtn.disabled = false;
        if (fileInput) fileInput.disabled = false;
    }

    async function uploadFilesSequential(files) {
        var total = files.length;
        var uploaded = 0;
        var lastOkName = "";

        for (var i = 0; i < total; i++) {
            var file = files[i];
            setUploadInFlight(i, total, file.name);
            var fd = new FormData();
            fd.append("files", file);
            try {
                var response = await fetch(uploadUrl, {
                    method: "POST",
                    credentials: "same-origin",
                    body: fd,
                });
                if (!response.ok) {
                    var text = await response.text();
                    showUploadError(file.name, text);
                    continue;
                }
                var data;
                try {
                    data = await response.json();
                } catch (eJson) {
                    showUploadError(file.name, "Respuesta no es JSON válido.");
                    continue;
                }
                if (!data.ok) {
                    showUploadError(file.name, data.error || data.message || "Error");
                    continue;
                }
                uploaded++;
                lastOkName = data.name || file.name;
                updateProgress(uploaded, total, file.name);
                refreshList();
            } catch (err) {
                showUploadError(file.name, err && err.message ? err.message : String(err));
            }
        }

        showUploadComplete(uploaded, total);
        if (lastOkName) {
            selected = lastOkName;
        }
        if (uploaded < total) {
            progressText.textContent +=
                uploaded === 0
                    ? " Ningún archivo se pudo subir."
                    : " Algunos archivos fallaron; revisá la consola para detalles.";
        }
        refreshList();
    }

    if (uploadBtn && fileInput) {
        uploadBtn.addEventListener("click", async function () {
            if (!fileInput.files || !fileInput.files.length) {
                fileInput.click();
                return;
            }
            var files = Array.prototype.slice.call(fileInput.files, 0);
            disableUploadUI();
            try {
                await uploadFilesSequential(files);
            } catch (eTop) {
                console.error(eTop);
            } finally {
                enableUploadUI();
                fileInput.value = "";
                renderSelectedFiles();
            }
        });
    }

    if (goToProcess) {
        goToProcess.addEventListener("click", function () {
            if (goToProcess.disabled) return;
            window.location.href = runUrl;
        });
    }

    refreshList();
})();
