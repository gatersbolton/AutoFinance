function toggleModePanels() {
    const checked = document.querySelector('input[name="mode"]:checked');
    const activeMode = checked ? checked.value : "existing_ocr_outputs";
    document.querySelectorAll("[data-mode-panel]").forEach((panel) => {
        panel.hidden = panel.getAttribute("data-mode-panel") !== activeMode;
    });
}

function parseBbox(rawValue) {
    if (!rawValue) {
        return null;
    }
    try {
        const points = JSON.parse(rawValue);
        if (!Array.isArray(points) || points.length === 0) {
            return null;
        }
        const xs = [];
        const ys = [];
        if (points.length === 4 && points.every((point) => Number.isFinite(Number(point)))) {
            xs.push(Number(points[0]), Number(points[2]));
            ys.push(Number(points[1]), Number(points[3]));
        } else {
            points.forEach((point) => {
                if (Array.isArray(point) && point.length >= 2) {
                    xs.push(Number(point[0]));
                    ys.push(Number(point[1]));
                    return;
                }
                if (point && typeof point === "object") {
                    xs.push(Number(point.x ?? point.X));
                    ys.push(Number(point.y ?? point.Y));
                }
            });
        }
        const validXs = xs.filter(Number.isFinite);
        const validYs = ys.filter(Number.isFinite);
        if (!validXs.length || !validYs.length) {
            return null;
        }
        return {
            left: Math.min(...validXs),
            top: Math.min(...validYs),
            right: Math.max(...validXs),
            bottom: Math.max(...validYs),
        };
    } catch (error) {
        return null;
    }
}

function highlightSourceForCell(cell, root) {
    const image = root.querySelector("[data-source-page-image]");
    const highlight = root.querySelector("[data-source-highlight]");
    const zoomPanel = root.querySelector("[data-source-zoom]");
    const zoomWindow = root.querySelector("[data-source-zoom-window]");
    const zoomHighlight = root.querySelector("[data-source-zoom-highlight]");
    const status = root.querySelector("[data-highlight-status]");
    const page = root.querySelector("[data-current-page]");
    if (!image || !highlight) {
        return;
    }
    const bbox = parseBbox(cell.getAttribute("data-bbox"));
    if (page) {
        page.textContent = cell.getAttribute("data-page-no") || "未记录";
    }
    if (!bbox || !image.naturalWidth || !image.naturalHeight) {
        highlight.style.display = "none";
        if (zoomPanel) {
            zoomPanel.hidden = true;
        }
        if (status) {
            status.textContent = cell.getAttribute("data-missing-highlight-message") || root.getAttribute("data-highlight-missing-text") || "当前单元格未记录位置";
        }
        return;
    }
    const renderedWidth = image.clientWidth;
    const renderedHeight = image.clientHeight;
    const scaleX = renderedWidth / image.naturalWidth;
    const scaleY = renderedHeight / image.naturalHeight;
    highlight.style.display = "block";
    highlight.style.left = `${bbox.left * scaleX}px`;
    highlight.style.top = `${bbox.top * scaleY}px`;
    highlight.style.width = `${Math.max(18, (bbox.right - bbox.left) * scaleX)}px`;
    highlight.style.height = `${Math.max(18, (bbox.bottom - bbox.top) * scaleY)}px`;
    const frame = image.closest(".source-image-frame");
    if (frame) {
        const highlightCenterX = ((bbox.left + bbox.right) / 2) * scaleX;
        const highlightCenterY = ((bbox.top + bbox.bottom) / 2) * scaleY;
        frame.scrollTo({
            left: Math.max(0, highlightCenterX - (frame.clientWidth / 2)),
            top: Math.max(0, highlightCenterY - (frame.clientHeight / 2)),
            behavior: "smooth",
        });
    }
    if (zoomPanel && zoomWindow && zoomHighlight) {
        const zoomWindowWidth = Math.max(1, zoomWindow.clientWidth);
        const zoomWindowHeight = Math.max(1, zoomWindow.clientHeight);
        const sourceWidth = Math.max(1, bbox.right - bbox.left);
        const sourceHeight = Math.max(1, bbox.bottom - bbox.top);
        const horizontalPreviewWidth = Math.max(sourceWidth * 1.7, sourceWidth + 96);
        const zoom = Math.max(1.05, Math.min(2, (zoomWindowWidth * 0.86) / horizontalPreviewWidth));
        const centerX = ((bbox.left + bbox.right) / 2) * zoom;
        const centerY = ((bbox.top + bbox.bottom) / 2) * zoom;
        const bboxWidth = Math.max(18, sourceWidth * zoom);
        const bboxHeight = Math.max(12, (bbox.bottom - bbox.top) * zoom);
        zoomPanel.hidden = false;
        zoomWindow.style.backgroundImage = `url("${image.currentSrc || image.src}")`;
        zoomWindow.style.backgroundSize = `${image.naturalWidth * zoom}px ${image.naturalHeight * zoom}px`;
        zoomWindow.style.backgroundPosition = `${(zoomWindowWidth / 2) - centerX}px ${(zoomWindowHeight / 2) - centerY}px`;
        zoomHighlight.style.width = `${bboxWidth}px`;
        zoomHighlight.style.height = `${bboxHeight}px`;
        zoomHighlight.style.left = `${(zoomWindowWidth - bboxWidth) / 2}px`;
        zoomHighlight.style.top = `${(zoomWindowHeight - bboxHeight) / 2}px`;
        zoomWindow.dataset.zoomScale = zoom.toFixed(3);
        zoomWindow.dataset.previewWidth = horizontalPreviewWidth.toFixed(2);
    }
    if (status) {
        status.textContent = root.getAttribute("data-highlight-found-text") || "已高亮选中单元格出处";
    }
}

function collectSheetEdits(root) {
    const rowLabels = new Map();
    root.querySelectorAll("[data-row-label]").forEach((label) => {
        rowLabels.set(label.getAttribute("data-row-index") || "", label.textContent.trim());
    });
    return Array.from(root.querySelectorAll("[data-editable-cell]")).map((cell) => ({
        review_item_id: cell.getAttribute("data-review-item-id") || "",
        raw_metric_id: cell.getAttribute("data-raw-metric-id") || "",
        row_index: cell.getAttribute("data-row-index") || "",
        col_index: cell.getAttribute("data-col-index") || "",
        metric_name: rowLabels.get(cell.getAttribute("data-row-index") || "") || cell.getAttribute("data-metric-name") || "",
        value: cell.textContent.trim(),
    }));
}

function centerSelectedSheetTab(root) {
    const tabs = root.querySelector("[data-sheet-tabs]");
    const selected = tabs ? tabs.querySelector("[data-selected-sheet-tab]") : null;
    if (!tabs || !selected) {
        return;
    }
    const tabsRect = tabs.getBoundingClientRect();
    const selectedRect = selected.getBoundingClientRect();
    const selectedCenter = (selectedRect.left - tabsRect.left) + tabs.scrollLeft + (selectedRect.width / 2);
    const targetLeft = selectedCenter - (tabs.clientWidth / 2);
    tabs.scrollTo({ left: Math.max(0, targetLeft), behavior: "auto" });
}

function initRawReviewSheet() {
    const root = document.querySelector("[data-raw-review-workbench]");
    if (!root) {
        return;
    }
    const cells = Array.from(root.querySelectorAll("[data-editable-cell]"));
    const selectedInput = root.querySelector("[data-selected-review-item]");
    const editsInput = root.querySelector("[data-edits-json]");
    const summary = root.querySelector("[data-selection-summary]");
    const form = root.querySelector("[data-raw-sheet-form]");

    function selectCell(cell) {
        cells.forEach((candidate) => candidate.classList.remove("metric-cell--selected"));
        cell.classList.add("metric-cell--selected");
        if (selectedInput) {
            selectedInput.value = cell.getAttribute("data-review-item-id") || selectedInput.value;
        }
        if (summary) {
            const name = cell.getAttribute("data-metric-name") || "未记录指标";
            summary.textContent = `当前选中：${name} / ${cell.textContent.trim()}`;
        }
        highlightSourceForCell(cell, root);
    }

    cells.forEach((cell) => {
        cell.addEventListener("click", () => selectCell(cell));
        cell.addEventListener("focus", () => selectCell(cell));
        cell.addEventListener("input", () => {
            cell.classList.add("metric-cell--dirty");
            if (summary && cell.classList.contains("metric-cell--selected")) {
                const name = cell.getAttribute("data-metric-name") || "未记录指标";
                summary.textContent = `当前选中：${name} / ${cell.textContent.trim()}`;
            }
        });
    });

    const initiallySelected = root.querySelector(".metric-cell--selected") || cells[0];
    const image = root.querySelector("[data-source-page-image]");
    if (image) {
        image.addEventListener("load", () => {
            if (initiallySelected) {
                highlightSourceForCell(initiallySelected, root);
            }
        });
    }
    if (initiallySelected) {
        selectCell(initiallySelected);
    }
    requestAnimationFrame(() => centerSelectedSheetTab(root));
    window.addEventListener("resize", () => centerSelectedSheetTab(root));

    if (form && editsInput) {
        form.addEventListener("submit", () => {
            editsInput.value = JSON.stringify(collectSheetEdits(root));
        });
    }
}

function closeAutocompleteResults(root) {
    root.querySelectorAll("[data-autocomplete-results]").forEach((panel) => {
        panel.hidden = true;
        panel.replaceChildren();
    });
}

function updateMappingRowSelection(row, root) {
    const rows = Array.from(root.querySelectorAll("[data-mapping-cell]"));
    const summary = root.querySelector("[data-selection-summary]");
    rows.forEach((candidate) => candidate.classList.remove("mapping-row--selected"));
    row.classList.add("mapping-row--selected");
    if (summary) {
        const originalName = row.getAttribute("data-original-metric-name") || "未记录术语";
        const mappingLabel = row.getAttribute("data-current-mapping-label") || "未映射";
        summary.textContent = `当前选中：${originalName} / ${mappingLabel}`;
    }
    const image = root.querySelector("[data-source-page-image]");
    const nextImageUrl = row.getAttribute("data-page-image-url") || "";
    if (image && nextImageUrl && image.getAttribute("src") !== nextImageUrl) {
        image.addEventListener("load", () => highlightSourceForCell(row, root), { once: true });
        image.setAttribute("src", nextImageUrl);
        return;
    }
    highlightSourceForCell(row, root);
}

function setMappingTerm(row, result) {
    const input = row.querySelector("[data-standard-term-input]");
    const selectedCode = row.querySelector("[data-selected-code]");
    const selectedName = row.querySelector("[data-selected-name]");
    const statusLabel = row.querySelector("[data-mapping-status-label]");
    const displayLabel = `${result.code} ${result.name}`.trim();
    if (input) {
        input.value = displayLabel;
    }
    if (selectedCode) {
        selectedCode.value = result.code || "";
    }
    if (selectedName) {
        selectedName.value = result.name || "";
    }
    row.setAttribute("data-current-mapping-label", displayLabel || "未映射");
    if ((result.code || "") !== (row.getAttribute("data-previous-code") || "")) {
        row.classList.add("mapping-row--dirty");
        if (statusLabel) {
            statusLabel.textContent = "已修改";
        }
    }
}

function renderAutocompleteResults(row, input, results, root) {
    const panel = row.querySelector("[data-autocomplete-results]");
    if (!panel) {
        return;
    }
    panel.replaceChildren();
    if (!results.length) {
        const empty = document.createElement("div");
        empty.className = "autocomplete-empty";
        empty.textContent = "没有找到标准术语";
        panel.appendChild(empty);
        panel.hidden = false;
        return;
    }
    results.forEach((result) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "autocomplete-option";
        const label = document.createElement("span");
        label.textContent = result.display_label || `${result.code} ${result.name}`;
        const reason = document.createElement("small");
        reason.textContent = result.match_reason || "";
        button.append(label, reason);
        button.addEventListener("click", () => {
            setMappingTerm(row, result);
            closeAutocompleteResults(root);
            input.focus();
        });
        panel.appendChild(button);
    });
    panel.hidden = false;
}

function initStandardTermAutocomplete(input, row, root) {
    let requestToken = 0;
    async function search() {
        const query = input.value.trim();
        if (!query) {
            closeAutocompleteResults(root);
            return;
        }
        const token = ++requestToken;
        try {
            const basePath = window.__APP_BASE_PATH__ || "";
            const response = await fetch(`${basePath}/api/standard-terms/search?q=${encodeURIComponent(query)}&limit=10`, {
                headers: { accept: "application/json" },
            });
            if (!response.ok || token !== requestToken) {
                return;
            }
            const payload = await response.json();
            renderAutocompleteResults(row, input, Array.isArray(payload.results) ? payload.results : [], root);
        } catch (error) {
            closeAutocompleteResults(root);
        }
    }
    input.addEventListener("input", search);
    input.addEventListener("focus", () => {
        updateMappingRowSelection(row, root);
        search();
    });
    input.addEventListener("click", () => updateMappingRowSelection(row, root));
}

function initMappingReviewSheet() {
    const root = document.querySelector("[data-mapping-review-workbench]");
    if (!root) {
        return;
    }
    const rows = Array.from(root.querySelectorAll("[data-mapping-cell]"));
    rows.forEach((row) => {
        row.addEventListener("click", () => updateMappingRowSelection(row, root));
        row.addEventListener("focusin", () => updateMappingRowSelection(row, root));
        const input = row.querySelector("[data-standard-term-input]");
        if (input) {
            initStandardTermAutocomplete(input, row, root);
        }
    });
    const initiallySelected = root.querySelector(".mapping-row--selected") || rows[0];
    const image = root.querySelector("[data-source-page-image]");
    if (image) {
        image.addEventListener("load", () => {
            if (initiallySelected) {
                highlightSourceForCell(initiallySelected, root);
            }
        });
    }
    if (initiallySelected) {
        updateMappingRowSelection(initiallySelected, root);
    }
    document.addEventListener("click", (event) => {
        if (!root.contains(event.target)) {
            closeAutocompleteResults(root);
        }
    });
}

document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll('input[name="mode"]').forEach((input) => {
        input.addEventListener("change", toggleModePanels);
    });
    toggleModePanels();
    initRawReviewSheet();
    initMappingReviewSheet();
});
