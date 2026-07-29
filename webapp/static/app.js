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

function updateSourceZoomPreview(image, zoomPanel, zoomWindow, zoomHighlight, bbox) {
    zoomPanel.hidden = false;
    const windowRect = zoomWindow.getBoundingClientRect();
    const zoomWindowWidth = Math.max(1, zoomWindow.clientWidth || windowRect.width);
    const zoomWindowHeight = Math.max(1, zoomWindow.clientHeight || windowRect.height);
    const sourceWidth = Math.max(1, bbox.right - bbox.left);
    const sourceHeight = Math.max(1, bbox.bottom - bbox.top);
    const horizontalPreviewWidth = Math.max(sourceWidth * 1.7, sourceWidth + 96);
    const zoom = Math.max(1.05, Math.min(2, (zoomWindowWidth * 0.86) / horizontalPreviewWidth));
    const centerX = ((bbox.left + bbox.right) / 2) * zoom;
    const centerY = ((bbox.top + bbox.bottom) / 2) * zoom;
    const bboxWidth = Math.max(18, sourceWidth * zoom);
    const bboxHeight = Math.max(12, sourceHeight * zoom);
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

function sourceImageMatches(image, nextImageUrl, nextImageKey) {
    if (!image) {
        return false;
    }
    const currentKey = image.dataset.pageImageKey || "";
    if (nextImageKey && currentKey === nextImageKey) {
        return true;
    }
    return !nextImageKey && nextImageUrl && image.getAttribute("src") === nextImageUrl;
}

function ensureSourceImageReady(image, nextImageUrl, nextImageKey, callback) {
    if (!image || !nextImageUrl) {
        callback();
        return;
    }
    const sameImage = sourceImageMatches(image, nextImageUrl, nextImageKey);
    if (sameImage) {
        if (image.complete && image.naturalWidth) {
            callback();
        } else {
            image.addEventListener("load", callback, { once: true });
        }
        return;
    }
    image.addEventListener("load", callback, { once: true });
    if (nextImageKey) {
        image.dataset.pageImageKey = nextImageKey;
    }
    image.setAttribute("src", nextImageUrl);
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
            behavior: "auto",
        });
    }
    if (zoomPanel && zoomWindow && zoomHighlight) {
        const wasHidden = zoomPanel.hidden;
        const zoomToken = `${cell.getAttribute("data-review-item-id") || ""}|${cell.getAttribute("data-raw-metric-id") || ""}|${cell.getAttribute("data-bbox") || ""}`;
        zoomPanel.dataset.zoomToken = zoomToken;
        updateSourceZoomPreview(image, zoomPanel, zoomWindow, zoomHighlight, bbox);
        if (wasHidden) {
            window.requestAnimationFrame(() => {
                if (!zoomPanel.hidden && zoomPanel.dataset.zoomToken === zoomToken) {
                    updateSourceZoomPreview(image, zoomPanel, zoomWindow, zoomHighlight, bbox);
                }
            });
        }
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

function closeAutocompleteResults(root = document) {
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
    const nextImageKey = row.getAttribute("data-page-image-key") || "";
    const highlightToken = [
        row.getAttribute("data-review-item-id") || "",
        row.getAttribute("data-raw-metric-id") || "",
        row.getAttribute("data-bbox") || "",
        String(window.performance ? window.performance.now() : Date.now()),
    ].join("|");
    root.dataset.highlightToken = highlightToken;
    const applyHighlight = () => {
        if (root.dataset.highlightToken === highlightToken) {
            highlightSourceForCell(row, root);
        }
    };
    if (image && nextImageUrl) {
        ensureSourceImageReady(image, nextImageUrl, nextImageKey, applyHighlight);
        return;
    }
    applyHighlight();
}

function setMappingTerm(row, result) {
    const input = row.querySelector("[data-standard-term-input]");
    const selectedCode = row.querySelector("[data-selected-code]");
    const selectedName = row.querySelector("[data-selected-name]");
    const decisionCodes = row.querySelectorAll("[data-decision-selected-code]");
    const decisionNames = row.querySelectorAll("[data-decision-selected-name]");
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
    decisionCodes.forEach((field) => {
        field.value = result.code || "";
    });
    decisionNames.forEach((field) => {
        field.value = result.name || "";
    });
    row.setAttribute("data-current-mapping-label", displayLabel || "未映射");
    if (!row.matches("[data-unified-row]") && (result.code || "") !== (row.getAttribute("data-previous-code") || "")) {
        row.classList.add("mapping-row--dirty");
        if (statusLabel) {
            statusLabel.textContent = "术语已修改";
        }
    }
    if (row.matches("[data-unified-row]")) {
        markUnifiedMappingChanged(row, result);
    }
}

function syncMappingDecisionForm(form) {
    const row = form.closest("[data-unified-row], [data-mapping-cell]");
    if (!row) {
        return;
    }
    const picker = row.querySelector("[data-mapping-picker]");
    const selectedCode = form.querySelector("[data-decision-selected-code]") || form.querySelector("[data-selected-code]");
    const selectedName = form.querySelector("[data-decision-selected-name]") || form.querySelector("[data-selected-name]");
    const code = picker ? (picker.dataset.currentCode || picker.getAttribute("data-current-code") || "") : (row.querySelector("[data-selected-code]")?.value || "");
    const name = picker ? (picker.dataset.currentName || picker.getAttribute("data-current-name") || "") : (row.querySelector("[data-selected-name]")?.value || "");
    if (selectedCode) {
        selectedCode.value = code;
    }
    if (selectedName) {
        selectedName.value = name;
    }
}

function renderAutocompleteResults(row, input, results, root, query) {
    const panel = row.querySelector("[data-autocomplete-results]");
    if (!panel) {
        return;
    }
    panel.replaceChildren();
    if (!query.trim()) {
        panel.hidden = true;
        return;
    }
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
        const chooseResult = (event) => {
            event.preventDefault();
            setMappingTerm(row, result);
            closeAutocompleteResults(root);
            input.focus();
        };
        button.addEventListener("mousedown", chooseResult);
        button.addEventListener("click", chooseResult);
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
            renderAutocompleteResults(row, input, Array.isArray(payload.results) ? payload.results : [], root, query);
        } catch (error) {
            closeAutocompleteResults(root);
        }
    }
    input.addEventListener("input", search);
    input.addEventListener("focus", () => {
        if (root.matches("[data-mapping-review-workbench]")) {
            updateMappingRowSelection(row, root);
        } else if (root.matches("[data-unified-proofread-workbench]")) {
            selectUnifiedRow(row, input);
            updateMappingResetVisibility(input);
        }
    });
    input.addEventListener("click", () => {
        if (root.matches("[data-mapping-review-workbench]")) {
            updateMappingRowSelection(row, root);
        } else if (root.matches("[data-unified-proofread-workbench]")) {
            selectUnifiedRow(row, input);
            updateMappingResetVisibility(input);
        }
    });
    input.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeAutocompleteResults(root);
            input.blur();
        }
    });
    input.addEventListener("blur", () => {
        window.setTimeout(() => {
            closeAutocompleteResults(root);
            if (root.matches("[data-unified-proofread-workbench]")) {
                updateMappingResetVisibility(input);
            }
        }, 120);
    });
}

function formatBulkThreshold(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return "90%";
    }
    const percent = numeric <= 1 ? numeric * 100 : numeric;
    return `${Math.round(percent * 10) / 10}%`;
}

function formatConfidenceLabel(label, value) {
    return `${label} 置信度${formatBulkThreshold(value)}`;
}

function renderBulkConfidencePreview(panel, preview) {
    panel.replaceChildren();
    panel.hidden = false;
    panel.dataset.previewState = "ready";

    const title = document.createElement("strong");
    title.textContent = `预览：AI置信度不低于 ${formatBulkThreshold(preview.threshold)} 的本次采纳`;
    panel.appendChild(title);

    const summary = document.createElement("p");
    summary.textContent = `可本次采纳 ${preview.eligible_total || 0} 条，排除 ${preview.excluded_total || 0} 条。本操作只对当前文件生效，不会写入本地映射库。`;
    panel.appendChild(summary);

    const excludedReasons = preview.excluded_reasons || {};
    const reasonEntries = Object.entries(excludedReasons).filter((entry) => Number(entry[1]) > 0);
    if (reasonEntries.length) {
        const reasons = document.createElement("p");
        reasons.className = "bulk-confidence-preview__reasons";
        reasons.textContent = `排除原因：${reasonEntries.map(([key, count]) => `${bulkConfidenceReasonLabel(key)} ${count} 条`).join("，")}`;
        panel.appendChild(reasons);
    }

    const candidates = Array.isArray(preview.candidates) ? preview.candidates : [];
    if (candidates.length) {
        const list = document.createElement("ul");
        candidates.slice(0, 5).forEach((candidate) => {
            const item = document.createElement("li");
            const rawName = candidate.raw_metric_name || candidate.original_metric_name || candidate.raw_metric_id || "未记录术语";
            const standardName = candidate.selected_name || candidate.candidate_name || candidate.standard_name || candidate.selected_code || candidate.candidate_code || "未记录标准术语";
            const confidenceValue = candidate.confidence ?? candidate.candidate_score;
            const confidence = confidenceValue !== undefined ? ` · ${formatConfidenceLabel("词语映射", confidenceValue)}` : "";
            item.textContent = `${rawName}：本次采纳为 ${standardName}${confidence}`;
            list.appendChild(item);
        });
        if (candidates.length > 5) {
            const more = document.createElement("li");
            more.textContent = `另有 ${candidates.length - 5} 条候选未展开。`;
            list.appendChild(more);
        }
        panel.appendChild(list);
    }
}

function bulkConfidenceReasonLabel(reason) {
    const labels = {
        already_decided: "已经人工处理过",
        status_not_reviewable: "当前状态不需要批量采纳",
        unsafe_relation_type: "术语关系不够直接，需要人工单独判断",
        missing_candidate_code: "没有可采纳的标准术语",
        confidence_below_threshold: "置信度低于当前阈值",
    };
    return labels[reason] || "其他未归类原因";
}

function renderBulkConfidenceError(panel, message) {
    panel.replaceChildren();
    panel.hidden = false;
    panel.dataset.previewState = "ready";
    const text = document.createElement("p");
    text.textContent = message;
    panel.appendChild(text);
}

function hideBulkConfidencePreview(root) {
    const panel = root.querySelector("[data-bulk-confidence-preview]");
    if (panel) {
        panel.replaceChildren();
        panel.hidden = true;
        panel.dataset.previewState = "";
    }
    setBulkConfidenceApplyEnabled(root, false);
    root.querySelectorAll("[data-bulk-confidence-preview-form] button[type='submit']").forEach((button) => {
        button.textContent = button.dataset.defaultLabel || "生成采纳预览";
    });
}

function markBulkConfidencePreviewVisible(root, preview = {}) {
    setBulkConfidenceApplyEnabled(root, Number(preview.eligible_total || 0) > 0);
    root.querySelectorAll("[data-bulk-confidence-preview-form] button[type='submit']").forEach((button) => {
        button.textContent = "收起预览";
    });
}

function setBulkConfidenceApplyEnabled(root, enabled) {
    root.querySelectorAll("[data-bulk-confidence-apply-button]").forEach((button) => {
        button.disabled = !enabled;
        button.setAttribute("aria-disabled", enabled ? "false" : "true");
    });
}

function syncBulkConfidenceThresholds(root, value) {
    root.querySelectorAll("[data-bulk-confidence-apply-form] input[name='threshold']").forEach((input) => {
        input.value = value;
    });
}

function initBulkConfidenceControls(root) {
    const previewPanel = root.querySelector("[data-bulk-confidence-preview]");
    setBulkConfidenceApplyEnabled(root, false);
    root.querySelectorAll("[data-bulk-threshold-input]").forEach((input) => {
        syncBulkConfidenceThresholds(root, input.value || "90");
        input.addEventListener("input", () => {
            syncBulkConfidenceThresholds(root, input.value || "90");
            hideBulkConfidencePreview(root);
        });
    });
    root.querySelectorAll("[data-bulk-confidence-preview-form]").forEach((form) => {
        const button = form.querySelector("button[type='submit']");
        if (button && !button.dataset.defaultLabel) {
            button.dataset.defaultLabel = button.textContent.trim() || "生成采纳预览";
        }
        form.addEventListener("submit", async (event) => {
            event.preventDefault();
            if (!previewPanel) {
                return;
            }
            if (!previewPanel.hidden && previewPanel.dataset.previewState === "ready") {
                hideBulkConfidencePreview(root);
                return;
            }
            if (button) {
                button.disabled = true;
            }
            renderBulkConfidenceError(previewPanel, "正在生成预览...");
            previewPanel.dataset.previewState = "loading";
            try {
                const url = new URL(form.action, window.location.href);
                const params = new URLSearchParams(new FormData(form));
                params.forEach((value, key) => url.searchParams.set(key, value));
                const response = await fetch(url.toString(), {
                    method: "GET",
                    headers: { "Accept": "application/json" },
                    credentials: "same-origin",
                });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const preview = await response.json();
                renderBulkConfidencePreview(previewPanel, preview);
                markBulkConfidencePreviewVisible(root, preview);
            } catch (error) {
                renderBulkConfidenceError(previewPanel, "预览生成失败，请稍后重试。");
                markBulkConfidencePreviewVisible(root, { eligible_total: 0 });
            } finally {
                if (button) {
                    button.disabled = false;
                }
            }
        });
    });
    root.querySelectorAll("[data-bulk-confidence-apply-form]").forEach((form) => {
        form.addEventListener("submit", (event) => {
            const button = form.querySelector("[data-bulk-confidence-apply-button]");
            if (button && button.disabled) {
                event.preventDefault();
                if (previewPanel) {
                    renderBulkConfidenceError(previewPanel, "请先生成采纳预览。");
                }
            }
        });
    });
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
        row.querySelectorAll("[data-mapping-decision-form], [data-mapping-action-form]").forEach((form) => {
            form.addEventListener("submit", () => syncMappingDecisionForm(form));
        });
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
    initBulkConfidenceControls(root);
    document.addEventListener("click", (event) => {
        if (!event.target.closest(".standard-term-picker")) {
            closeAutocompleteResults(root);
        }
    });
}

function formatMetricNumber(value, valueType = "") {
    const text = String(value ?? "").trim();
    if (!text) {
        return "";
    }
    const parsed = parseMetricNumberInput(text, valueType);
    if (!parsed.valid) {
        return text;
    }
    return formatCanonicalMetricNumber(parsed.value);
}

function formatCanonicalMetricNumber(value) {
    const match = String(value || "").match(/^([+-]?)(\d+)\.(\d{2})$/);
    if (!match) {
        return String(value || "");
    }
    const sign = match[1] || "";
    const integer = match[2].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    return `${sign}${integer}.${match[3]}`;
}

function incrementIntegerString(value) {
    const digits = String(value || "0").split("");
    let carry = 1;
    for (let index = digits.length - 1; index >= 0; index -= 1) {
        const next = Number(digits[index]) + carry;
        if (next >= 10) {
            digits[index] = "0";
            carry = 1;
        } else {
            digits[index] = String(next);
            carry = 0;
            break;
        }
    }
    if (carry) {
        digits.unshift("1");
    }
    return digits.join("");
}

function canonicalizeMetricNumber(sign, intPart, fracPart) {
    let integer = (intPart || "0").replace(/^0+(?=\d)/, "") || "0";
    const rawFraction = String(fracPart || "");
    const padded = rawFraction.padEnd(3, "0");
    let cents = Number(padded.slice(0, 2));
    if (Number(padded[2]) >= 5) {
        cents += 1;
    }
    if (cents >= 100) {
        integer = incrementIntegerString(integer);
        cents -= 100;
    }
    const fraction = String(cents).padStart(2, "0");
    return {
        value: `${sign || ""}${integer}.${fraction}`,
        precisionAdjusted: rawFraction.length > 2 && /[1-9]/.test(rawFraction.slice(2)),
    };
}

function parseMetricNumberInput(value, valueType = "") {
    const text = String(value ?? "").trim();
    if (!text) {
        return { valid: false, value: "", reason: "empty" };
    }
    const normalizedType = String(valueType || "").toLowerCase();
    if (text.includes("%") || ["ratio", "percentage", "percent"].includes(normalizedType)) {
        return { valid: false, value: text, reason: "ratio_or_percent_requires_explicit_handling" };
    }
    const normalized = text.replace(/[,，\s]/g, "");
    const match = normalized.match(/^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))$/);
    if (!match) {
        return { valid: false, value: text, reason: "invalid_number" };
    }
    const sign = match[1] || "";
    if (match[4] !== undefined) {
        const result = canonicalizeMetricNumber(sign, "0", match[4]);
        return { valid: true, value: result.value, reason: "", precisionAdjusted: result.precisionAdjusted };
    }
    const intPart = (match[2] || "0").replace(/^0+(?=\d)/, "") || "0";
    const result = canonicalizeMetricNumber(sign, intPart, match[3] || "");
    return { valid: true, value: result.value, reason: "", precisionAdjusted: result.precisionAdjusted };
}

function statusBadge(code, label) {
    const badge = document.createElement("span");
    badge.className = `status status--mapping status--mapping-${code}`;
    badge.dataset.statusBadge = "";
    badge.textContent = label;
    return badge;
}

function updateUnifiedStatus(row) {
    const list = row.querySelector("[data-status-badges]");
    if (!list) {
        return;
    }
    list.replaceChildren();
    if (row.dataset.valueChanged === "true") {
        list.appendChild(statusBadge("value_changed", "数值已修改"));
    }
    if (row.dataset.unitChanged === "true") {
        list.appendChild(statusBadge("unit_changed", "单位已修改"));
    }
    if (row.dataset.dateChanged === "true") {
        list.appendChild(statusBadge("date_changed", "日期已修改"));
    }
    if (row.dataset.mappingChanged === "true") {
        list.appendChild(statusBadge("term_changed", "术语已修改"));
    }
    if (row.dataset.temporalReviewRequired === "true") {
        list.appendChild(statusBadge("review_required", "日期待校对"));
    }
    if (!list.children.length) {
        list.appendChild(statusBadge(row.dataset.baseStatusCode || "unmapped", row.dataset.baseStatusLabel || "未映射"));
    }
}

function isValueChangedFromOriginal(input) {
    return (input.dataset.currentValue || "") !== (input.getAttribute("data-original-value") || "");
}

function rowHasChangedMetricValue(row) {
    return Array.from(row.querySelectorAll("[data-metric-value-input]")).some((input) => isValueChangedFromOriginal(input));
}

function isDateChangedFromOriginal(input) {
    return (input.dataset.currentDate || "") !== (input.getAttribute("data-original-date") || "");
}

function isUnitChangedFromOriginal(select) {
    return (select.dataset.currentUnit || select.value || "") !== (select.getAttribute("data-original-unit") || "");
}

function isMappingChangedFromOriginal(picker) {
    return (picker.dataset.currentCode || "") !== (picker.getAttribute("data-original-code") || "")
        || (picker.dataset.currentName || "") !== (picker.getAttribute("data-original-name") || "");
}

function updateValueResetVisibility(input) {
    const cell = input.closest("[data-value-cell]");
    const reset = cell ? cell.querySelector("[data-reset-value]") : null;
    if (reset) {
        reset.hidden = !(document.activeElement === input && isValueChangedFromOriginal(input));
    }
}

function updateDateResetVisibility(input) {
    const cell = input.closest("[data-date-cell]");
    const reset = cell ? cell.querySelector("[data-reset-date]") : null;
    if (reset) {
        reset.hidden = !isDateChangedFromOriginal(input);
    }
}

function updateUnitResetVisibility(select) {
    const cell = select.closest("[data-value-cell]");
    const reset = cell ? cell.querySelector("[data-reset-unit]") : null;
    if (reset) {
        reset.hidden = !isUnitChangedFromOriginal(select);
    }
}

function updateValuePrecisionNote(input, parsed = null) {
    const cell = input.closest("[data-value-cell]");
    const note = cell ? cell.querySelector("[data-value-precision-note]") : null;
    if (note) {
        note.hidden = !(parsed && parsed.precisionAdjusted);
    }
}

function updateMappingResetVisibility(input) {
    const picker = input.closest("[data-mapping-picker]");
    const reset = picker ? picker.querySelector("[data-reset-mapping]") : null;
    if (reset) {
        reset.hidden = !(document.activeElement === input && isMappingChangedFromOriginal(picker));
    }
}

function selectUnifiedRow(row, target) {
    const root = row.closest("[data-unified-proofread-workbench]");
    if (!root) {
        return;
    }
    root.querySelectorAll("[data-unified-row]").forEach((candidate) => {
        candidate.classList.remove("unified-row--selected");
    });
    row.classList.add("unified-row--selected");
    const summary = root.querySelector("[data-selection-summary]");
    if (summary) {
        const originalName = row.getAttribute("data-original-metric-name") || "未记录术语";
        const mappingLabel = row.getAttribute("data-current-mapping-label") || "未映射";
        summary.textContent = `当前选中：${originalName} / ${mappingLabel}`;
    }
    const image = root.querySelector("[data-source-page-image]");
    const nextImageUrl = row.getAttribute("data-page-image-url") || "";
    const nextImageKey = row.getAttribute("data-page-image-key") || "";
    const highlightTarget = target && target.closest("[data-value-cell]") ? target.closest("[data-value-cell]") : row;
    const highlightToken = [
        row.getAttribute("data-review-item-id") || "",
        row.getAttribute("data-raw-metric-id") || "",
        highlightTarget.getAttribute("data-bbox") || "",
        String(window.performance ? window.performance.now() : Date.now()),
    ].join("|");
    root.dataset.highlightToken = highlightToken;
    const applyHighlight = () => {
        if (root.dataset.highlightToken === highlightToken) {
            highlightSourceForCell(highlightTarget, root);
        }
    };
    if (image && nextImageUrl) {
        ensureSourceImageReady(image, nextImageUrl, nextImageKey, applyHighlight);
        return;
    }
    applyHighlight();
}

function markUnifiedValueChanged(row, input) {
    const cell = input.closest("[data-value-cell]");
    const error = cell ? cell.querySelector("[data-value-error]") : null;
    const parsed = parseMetricNumberInput(input.value, input.getAttribute("data-value-type") || "");
    if (!parsed.valid) {
        input.classList.add("metric-value-input--invalid");
        if (error) {
            error.hidden = false;
        }
        updateValuePrecisionNote(input, null);
        input.dataset.dirty = "false";
        return;
    }
    input.classList.remove("metric-value-input--invalid");
    if (error) {
        error.hidden = true;
    }
    input.dataset.currentValue = parsed.value;
    const originalValue = input.getAttribute("data-original-value") || "";
    const savedValue = input.dataset.savedValue || originalValue;
    const changedFromOriginal = parsed.value !== originalValue;
    const resetPending = parsed.value === originalValue && savedValue !== originalValue;
    input.dataset.dirty = parsed.value !== savedValue && !resetPending ? "true" : "false";
    if (cell) {
        cell.dataset.resetPending = resetPending ? "true" : "false";
        cell.classList.toggle("metric-cell--dirty", changedFromOriginal);
    }
    row.dataset.valueChanged = rowHasChangedMetricValue(row) ? "true" : "false";
    updateValuePrecisionNote(input, parsed);
    updateValueResetVisibility(input);
    updateUnifiedStatus(row);
}

function resetUnifiedValue(row, input) {
    const cell = input.closest("[data-value-cell]");
    const originalValue = input.getAttribute("data-original-value") || "";
    const savedValue = input.dataset.savedValue || originalValue;
    input.value = formatMetricNumber(originalValue, input.getAttribute("data-value-type") || "");
    input.dataset.currentValue = originalValue;
    input.dataset.dirty = "false";
    input.classList.remove("metric-value-input--invalid");
    if (cell) {
        cell.dataset.resetPending = savedValue !== originalValue ? "true" : "false";
        cell.classList.remove("metric-cell--dirty");
        const error = cell.querySelector("[data-value-error]");
        if (error) {
            error.hidden = true;
        }
        updateValuePrecisionNote(input, null);
    }
    row.dataset.valueChanged = rowHasChangedMetricValue(row) ? "true" : "false";
    updateValueResetVisibility(input);
    updateUnifiedStatus(row);
}

function markUnifiedDateChanged(row, input) {
    const cell = input.closest("[data-date-cell]");
    const error = cell ? cell.querySelector("[data-date-error]") : null;
    const currentDate = input.value || "";
    const originalDate = input.getAttribute("data-original-date") || "";
    const savedDate = input.dataset.savedDate || originalDate;
    const valid = /^\d{4}-\d{2}-\d{2}$/.test(currentDate);
    input.classList.toggle("metric-value-input--invalid", !valid);
    if (error) {
        error.hidden = valid;
    }
    if (!valid) {
        input.dataset.dirty = "false";
        return;
    }
    input.dataset.currentDate = currentDate;
    const resetPending = currentDate === originalDate && savedDate !== originalDate;
    input.dataset.dirty = currentDate !== savedDate && !resetPending ? "true" : "false";
    if (cell) {
        cell.dataset.resetPending = resetPending ? "true" : "false";
        cell.classList.toggle("metric-cell--dirty", currentDate !== originalDate);
    }
    row.dataset.dateChanged = currentDate !== originalDate ? "true" : "false";
    row.dataset.temporalReviewRequired = "false";
    updateDateResetVisibility(input);
    updateUnifiedStatus(row);
}

function resetUnifiedDate(row, input) {
    const cell = input.closest("[data-date-cell]");
    const originalDate = input.getAttribute("data-original-date") || "";
    const savedDate = input.dataset.savedDate || originalDate;
    input.value = originalDate;
    input.dataset.currentDate = originalDate;
    input.dataset.dirty = "false";
    input.classList.remove("metric-value-input--invalid");
    if (cell) {
        cell.dataset.resetPending = savedDate !== originalDate ? "true" : "false";
        cell.classList.remove("metric-cell--dirty");
        const error = cell.querySelector("[data-date-error]");
        if (error) {
            error.hidden = true;
        }
    }
    row.dataset.dateChanged = "false";
    row.dataset.temporalReviewRequired = row.dataset.originalTemporalReviewRequired || "false";
    updateDateResetVisibility(input);
    updateUnifiedStatus(row);
}

function normalizedYuanFromRaw(rawValue, unit) {
    let text = String(rawValue || "").trim();
    const negative = text.startsWith("(") && text.endsWith(")");
    if (negative) {
        text = `-${text.slice(1, -1)}`;
    }
    text = text.replace(/[￥¥元]/g, "");
    const parsed = parseMetricNumberInput(text, "amount");
    if (!parsed.valid) {
        return "";
    }
    const factors = { "元": 1n, "千元": 1000n, "万元": 10000n, "亿元": 100000000n };
    const factor = factors[unit];
    if (!factor) {
        return "";
    }
    const match = parsed.value.match(/^([+-]?)(\d+)\.(\d{2})$/);
    if (!match) {
        return "";
    }
    const cents = BigInt(`${match[2]}${match[3]}`) * factor;
    const digits = cents.toString().padStart(3, "0");
    const integer = digits.slice(0, -2);
    const fraction = digits.slice(-2);
    return `${match[1] || ""}${integer}.${fraction}`;
}

function markUnifiedUnitChanged(row, select) {
    const cell = select.closest("[data-value-cell]");
    const valueInput = cell ? cell.querySelector("[data-metric-value-input]") : null;
    const originalUnit = select.getAttribute("data-original-unit") || "";
    const savedUnit = select.dataset.savedUnit || originalUnit;
    const currentUnit = select.value || "";
    const resetPending = currentUnit === originalUnit && savedUnit !== originalUnit;
    select.dataset.currentUnit = currentUnit;
    select.dataset.dirty = currentUnit !== savedUnit && !resetPending ? "true" : "false";
    select.dataset.resetPending = resetPending ? "true" : "false";
    if (valueInput) {
        const normalized = normalizedYuanFromRaw(select.dataset.rawValue || "", currentUnit);
        if (normalized) {
            valueInput.value = formatMetricNumber(normalized, valueInput.getAttribute("data-value-type") || "");
            valueInput.dataset.currentValue = normalized;
            markUnifiedValueChanged(row, valueInput);
        }
    }
    row.dataset.unitChanged = isUnitChangedFromOriginal(select) ? "true" : "false";
    if (cell) {
        cell.classList.toggle("metric-cell--dirty", row.dataset.unitChanged === "true" || (valueInput && isValueChangedFromOriginal(valueInput)));
    }
    updateUnitResetVisibility(select);
    updateUnifiedStatus(row);
}

function resetUnifiedUnit(row, select) {
    const originalUnit = select.getAttribute("data-original-unit") || "元";
    const savedUnit = select.dataset.savedUnit || originalUnit;
    select.value = originalUnit;
    select.dataset.currentUnit = originalUnit;
    select.dataset.dirty = "false";
    select.dataset.resetPending = savedUnit !== originalUnit ? "true" : "false";
    markUnifiedUnitChanged(row, select);
    select.dataset.dirty = "false";
    select.dataset.resetPending = savedUnit !== originalUnit ? "true" : "false";
    row.dataset.unitChanged = "false";
    updateUnitResetVisibility(select);
    updateUnifiedStatus(row);
}

function markUnifiedMappingChanged(row, result) {
    const picker = row.querySelector("[data-mapping-picker]");
    const input = row.querySelector("[data-standard-term-input]");
    if (!picker) {
        return;
    }
    picker.dataset.currentCode = result.code || "";
    picker.dataset.currentName = result.name || "";
    const originalCode = picker.getAttribute("data-original-code") || "";
    const originalName = picker.getAttribute("data-original-name") || "";
    const savedCode = picker.dataset.savedCode || originalCode;
    const savedName = picker.dataset.savedName || originalName;
    const changedFromOriginal = (result.code || "") !== originalCode || (result.name || "") !== originalName;
    const resetPending = (result.code || "") === originalCode && (result.name || "") === originalName && (savedCode !== originalCode || savedName !== originalName);
    picker.dataset.dirty = ((result.code || "") !== savedCode || (result.name || "") !== savedName) && !resetPending ? "true" : "false";
    picker.dataset.resetPending = resetPending ? "true" : "false";
    picker.classList.toggle("mapping-row--dirty", changedFromOriginal);
    const label = `${result.code || ""} ${result.name || ""}`.trim();
    row.setAttribute("data-current-mapping-label", label || "未映射");
    row.dataset.mappingChanged = changedFromOriginal ? "true" : "false";
    if (input) {
        updateMappingResetVisibility(input);
    }
    updateUnifiedStatus(row);
}

function resetUnifiedMapping(row) {
    const picker = row.querySelector("[data-mapping-picker]");
    const input = row.querySelector("[data-standard-term-input]");
    const selectedCode = row.querySelector("[data-selected-code]");
    const selectedName = row.querySelector("[data-selected-name]");
    if (!picker || !input) {
        return;
    }
    const originalCode = picker.getAttribute("data-original-code") || "";
    const originalName = picker.getAttribute("data-original-name") || "";
    const savedCode = picker.dataset.savedCode || originalCode;
    const savedName = picker.dataset.savedName || originalName;
    const label = `${originalCode} ${originalName}`.trim();
    input.value = label;
    picker.dataset.currentCode = originalCode;
    picker.dataset.currentName = originalName;
    picker.dataset.dirty = "false";
    picker.dataset.resetPending = savedCode !== originalCode || savedName !== originalName ? "true" : "false";
    picker.classList.remove("mapping-row--dirty");
    row.setAttribute("data-current-mapping-label", label || "未映射");
    if (selectedCode) {
        selectedCode.value = originalCode;
    }
    if (selectedName) {
        selectedName.value = originalName;
    }
    row.dataset.mappingChanged = "false";
    updateMappingResetVisibility(input);
    updateUnifiedStatus(row);
}

function collectUnifiedEdits(root) {
    const edits = [];
    root.querySelectorAll("[data-unified-row]").forEach((row) => {
        const itemId = row.getAttribute("data-review-item-id") || "";
        const rawMetricId = row.getAttribute("data-raw-metric-id") || "";
        const dateInput = row.querySelector("[data-metric-date-input]");
        const dateCell = dateInput ? dateInput.closest("[data-date-cell]") : null;
        if (dateInput && dateInput.dataset.dirty === "true") {
            edits.push({
                item_id: itemId,
                raw_metric_id: rawMetricId,
                raw_metric_ids: (row.getAttribute("data-raw-metric-ids") || rawMetricId).split(",").filter(Boolean),
                edit_type: "date_change",
                previous_date: dateInput.dataset.savedDate || dateInput.getAttribute("data-original-date") || "",
                new_date: dateInput.dataset.currentDate || dateInput.value,
            });
        } else if (dateInput && dateCell && dateCell.dataset.resetPending === "true") {
            edits.push({
                item_id: itemId,
                raw_metric_id: rawMetricId,
                raw_metric_ids: (row.getAttribute("data-raw-metric-ids") || rawMetricId).split(",").filter(Boolean),
                edit_type: "reset_date",
                previous_date: dateInput.dataset.savedDate || "",
                new_date: dateInput.getAttribute("data-original-date") || "",
            });
        }
        row.querySelectorAll("[data-metric-value-input]").forEach((valueInput) => {
            const valueCell = valueInput.closest("[data-value-cell]");
            const inputRawMetricId = valueInput.getAttribute("data-raw-metric-id") || valueCell?.getAttribute("data-raw-metric-id") || rawMetricId;
            const unitSelect = valueCell ? valueCell.querySelector("[data-source-unit-select]") : null;
            if (!inputRawMetricId) {
                return;
            }
            if (unitSelect && unitSelect.dataset.dirty === "true") {
                edits.push({
                    item_id: itemId,
                    raw_metric_id: inputRawMetricId,
                    value_slot: valueInput.getAttribute("data-value-slot") || valueCell?.getAttribute("data-value-slot") || "",
                    edit_type: "unit_change",
                    previous_unit: unitSelect.dataset.savedUnit || unitSelect.getAttribute("data-original-unit") || "",
                    new_unit: unitSelect.dataset.currentUnit || unitSelect.value,
                    previous_value: valueInput.dataset.savedValue || valueInput.getAttribute("data-original-value") || "",
                    new_value: valueInput.dataset.currentValue || valueInput.value,
                });
            } else if (unitSelect && unitSelect.dataset.resetPending === "true") {
                edits.push({
                    item_id: itemId,
                    raw_metric_id: inputRawMetricId,
                    value_slot: valueInput.getAttribute("data-value-slot") || valueCell?.getAttribute("data-value-slot") || "",
                    edit_type: "reset_unit",
                    previous_unit: unitSelect.dataset.savedUnit || "",
                    new_unit: unitSelect.getAttribute("data-original-unit") || "元",
                    previous_value: valueInput.dataset.savedValue || "",
                    new_value: valueInput.getAttribute("data-original-value") || "",
                });
            } else if (valueInput.dataset.dirty === "true") {
                const savedValue = valueInput.dataset.savedValue || valueInput.getAttribute("data-original-value") || "";
                edits.push({
                    item_id: itemId,
                    raw_metric_id: inputRawMetricId,
                    value_slot: valueInput.getAttribute("data-value-slot") || valueCell?.getAttribute("data-value-slot") || "",
                    edit_type: "value_change",
                    previous_value: savedValue,
                    new_value: valueInput.dataset.currentValue || valueInput.value,
                });
            } else if (valueCell && valueCell.dataset.resetPending === "true") {
                const savedValue = valueInput.dataset.savedValue || valueInput.getAttribute("data-original-value") || "";
                edits.push({
                    item_id: itemId,
                    raw_metric_id: inputRawMetricId,
                    value_slot: valueInput.getAttribute("data-value-slot") || valueCell.getAttribute("data-value-slot") || "",
                    edit_type: "reset_value",
                    previous_value: savedValue,
                    new_value: valueInput.getAttribute("data-original-value") || "",
                });
            }
        });
        const picker = row.querySelector("[data-mapping-picker]");
        if (picker && picker.dataset.dirty === "true") {
            const savedCode = picker.dataset.savedCode || picker.getAttribute("data-original-code") || "";
            const savedName = picker.dataset.savedName || picker.getAttribute("data-original-name") || "";
            edits.push({
                item_id: itemId,
                raw_metric_id: rawMetricId,
                raw_metric_ids: (row.getAttribute("data-raw-metric-ids") || rawMetricId).split(",").filter(Boolean),
                edit_type: "mapping_change",
                previous_code: savedCode,
                previous_name: savedName,
                new_code: picker.dataset.currentCode || "",
                new_name: picker.dataset.currentName || "",
            });
        } else if (picker && picker.dataset.resetPending === "true") {
            const savedCode = picker.dataset.savedCode || picker.getAttribute("data-original-code") || "";
            const savedName = picker.dataset.savedName || picker.getAttribute("data-original-name") || "";
            edits.push({
                item_id: itemId,
                raw_metric_id: rawMetricId,
                raw_metric_ids: (row.getAttribute("data-raw-metric-ids") || rawMetricId).split(",").filter(Boolean),
                edit_type: "reset_mapping",
                previous_code: savedCode,
                previous_name: savedName,
                new_code: picker.getAttribute("data-original-code") || "",
                new_name: picker.getAttribute("data-original-name") || "",
            });
        }
    });
    return edits;
}

function numericDatasetValue(row, key) {
    const rawValue = row.getAttribute(key) || "";
    if (!rawValue) {
        return null;
    }
    const value = Number(rawValue);
    return Number.isFinite(value) ? value : null;
}

function unifiedStatusMatches(row, filterValue) {
    if (!filterValue || filterValue === "all") {
        return true;
    }
    const baseStatus = row.getAttribute("data-base-status-code") || "";
    const decisionNote = row.getAttribute("data-mapping-decision-note") || "";
    if (filterValue === "changed") {
        return row.dataset.valueChanged === "true"
            || row.dataset.mappingChanged === "true"
            || row.dataset.dateChanged === "true"
            || row.dataset.unitChanged === "true";
    }
    if (filterValue === "accept_once") {
        return decisionNote.includes("已本次采用");
    }
    if (filterValue === "review_required") {
        return row.dataset.temporalReviewRequired === "true"
            || ["review_required", "candidate", "relation_review"].includes(baseStatus);
    }
    return baseStatus === filterValue;
}

function unifiedConfidenceMatches(row, filterValue, threshold) {
    if (!filterValue || filterValue === "all") {
        return true;
    }
    const textConfidence = numericDatasetValue(row, "data-text-confidence");
    const valueConfidence = numericDatasetValue(row, "data-value-confidence");
    const textLow = textConfidence !== null && textConfidence < threshold;
    const valueLow = valueConfidence !== null && valueConfidence < threshold;
    if (filterValue === "text_low") {
        return textLow;
    }
    if (filterValue === "value_low") {
        return valueLow;
    }
    if (filterValue === "any_low") {
        return textLow || valueLow;
    }
    return true;
}

function unifiedStatusPriority(row) {
    const baseStatus = row.getAttribute("data-base-status-code") || "";
    const decisionNote = row.getAttribute("data-mapping-decision-note") || "";
    if (row.dataset.temporalReviewRequired === "true") {
        return 0;
    }
    if (baseStatus === "unmapped") {
        return 0;
    }
    if (baseStatus === "llm_suggested") {
        return 1;
    }
    if (["review_required", "candidate", "relation_review"].includes(baseStatus)) {
        return 2;
    }
    if (
        row.dataset.valueChanged === "true"
        || row.dataset.mappingChanged === "true"
        || row.dataset.dateChanged === "true"
        || row.dataset.unitChanged === "true"
    ) {
        return 3;
    }
    if (decisionNote.includes("已本次采用")) {
        return 4;
    }
    return 5;
}

function sortUnifiedRows(root) {
    const tbody = root.querySelector("[data-unified-table] tbody");
    if (!tbody) {
        return;
    }
    const sortValue = root.querySelector("[data-unified-sort]")?.value || "page";
    const rows = Array.from(root.querySelectorAll("[data-unified-row]"));
    const headers = Array.from(root.querySelectorAll("[data-section-header]"));
    if (sortValue === "page") {
        Array.from(tbody.children)
            .sort((left, right) => Number(left.dataset.originalDomOrder || "0") - Number(right.dataset.originalDomOrder || "0"))
            .forEach((child) => tbody.appendChild(child));
        headers.forEach((header) => {
            const sectionKey = header.getAttribute("data-section-key") || "";
            const hasVisibleRows = rows.some((row) => !row.hidden && (row.getAttribute("data-section-key") || "") === sectionKey);
            header.hidden = !hasVisibleRows;
        });
        root.querySelectorAll("[data-row-page-chip]").forEach((chip) => {
            chip.hidden = true;
        });
        return;
    }

    const sortedRows = rows.sort((left, right) => {
        if (sortValue === "value_confidence_asc" || sortValue === "text_confidence_asc") {
            const key = sortValue === "value_confidence_asc" ? "data-value-confidence" : "data-text-confidence";
            const leftValue = numericDatasetValue(left, key);
            const rightValue = numericDatasetValue(right, key);
            const leftSort = leftValue === null ? Number.POSITIVE_INFINITY : leftValue;
            const rightSort = rightValue === null ? Number.POSITIVE_INFINITY : rightValue;
            if (leftSort !== rightSort) {
                return leftSort - rightSort;
            }
        }
        if (sortValue === "status_priority") {
            const priorityDelta = unifiedStatusPriority(left) - unifiedStatusPriority(right);
            if (priorityDelta !== 0) {
                return priorityDelta;
            }
        }
        return Number(left.dataset.originalOrder || "0") - Number(right.dataset.originalOrder || "0");
    });
    headers.forEach((header) => {
        header.hidden = true;
        tbody.appendChild(header);
    });
    sortedRows.forEach((row) => tbody.appendChild(row));
    root.querySelectorAll("[data-row-page-chip]").forEach((chip) => {
        chip.hidden = false;
    });
}

function filterUnifiedRows(root) {
    const queryInput = root.querySelector("[data-unified-table-search]");
    const query = queryInput ? queryInput.value.trim().toLowerCase() : "";
    const statusFilter = root.querySelector("[data-unified-status-filter]")?.value || "all";
    const confidenceFilter = root.querySelector("[data-unified-confidence-filter]")?.value || "all";
    const thresholdRaw = Number(root.querySelector("[data-unified-confidence-threshold]")?.value || "90");
    const threshold = Number.isFinite(thresholdRaw) ? Math.max(0, Math.min(100, thresholdRaw)) / 100 : 0.9;
    const visibleSections = new Set();
    const rows = Array.from(root.querySelectorAll("[data-unified-row]"));
    rows.forEach((row) => {
        const text = row.textContent.toLowerCase();
        const visible = (!query || text.includes(query))
            && unifiedStatusMatches(row, statusFilter)
            && unifiedConfidenceMatches(row, confidenceFilter, threshold);
        row.hidden = !visible;
        if (visible) {
            visibleSections.add(row.getAttribute("data-section-key") || "");
        }
    });
    root.querySelectorAll("[data-section-header]").forEach((header) => {
        header.hidden = !visibleSections.has(header.getAttribute("data-section-key") || "");
    });
    sortUnifiedRows(root);
    const firstVisible = Array.from(root.querySelectorAll("[data-unified-row]")).find((row) => !row.hidden);
    if (firstVisible) {
        selectUnifiedRow(firstVisible, firstVisible);
    }
}

async function saveUnifiedEdits(root, options = {}) {
    const status = root.querySelector("[data-unified-save-status]");
    const button = root.querySelector("[data-unified-save]");
    const edits = options.edits || collectUnifiedEdits(root);
    if (!edits.length) {
        if (status) {
            status.textContent = "没有需要保存的修改";
        }
        return { pass: true, skipped: true };
    }
    if (button) {
        button.disabled = true;
    }
    if (status) {
        status.textContent = "正在保存...";
    }
    try {
        const response = await fetch(root.getAttribute("data-save-url") || "", {
            method: "POST",
            headers: { "content-type": "application/json", accept: "application/json" },
            body: JSON.stringify({ edits }),
        });
        if (!response.ok) {
            const payload = await response.json().catch(() => ({}));
            throw new Error(payload.detail || "保存失败");
        }
        const payload = await response.json();
        root.querySelectorAll("[data-unified-row]").forEach((row) => {
            const dateInput = row.querySelector("[data-metric-date-input]");
            const dateCell = dateInput ? dateInput.closest("[data-date-cell]") : null;
            if (dateInput) {
                const originalDate = dateInput.getAttribute("data-original-date") || "";
                const currentDate = dateInput.dataset.currentDate || dateInput.value || originalDate;
                if (dateCell && dateCell.dataset.resetPending === "true") {
                    dateInput.dataset.savedDate = originalDate;
                    dateInput.value = originalDate;
                    dateInput.dataset.currentDate = originalDate;
                } else if (dateInput.dataset.dirty === "true") {
                    dateInput.dataset.savedDate = currentDate;
                    dateInput.value = currentDate;
                }
                dateInput.dataset.dirty = "false";
                const changedFromOriginal = isDateChangedFromOriginal(dateInput);
                row.dataset.dateChanged = changedFromOriginal ? "true" : "false";
                row.dataset.temporalReviewRequired = changedFromOriginal
                    ? "false"
                    : (row.dataset.originalTemporalReviewRequired || "false");
                if (dateCell) {
                    dateCell.dataset.resetPending = "false";
                    dateCell.classList.toggle("metric-cell--dirty", changedFromOriginal);
                }
                updateDateResetVisibility(dateInput);
            }
            row.querySelectorAll("[data-metric-value-input]").forEach((valueInput) => {
                const valueCell = valueInput.closest("[data-value-cell]");
                const originalValue = valueInput.getAttribute("data-original-value") || "";
                const currentValue = valueInput.dataset.currentValue || originalValue;
                if (valueCell && valueCell.dataset.resetPending === "true") {
                    valueInput.dataset.savedValue = originalValue;
                    valueInput.value = formatMetricNumber(originalValue, valueInput.getAttribute("data-value-type") || "");
                } else if (valueInput.dataset.dirty === "true") {
                    valueInput.dataset.savedValue = currentValue;
                    valueInput.value = formatMetricNumber(currentValue, valueInput.getAttribute("data-value-type") || "");
                }
                valueInput.dataset.dirty = "false";
                const changedFromOriginal = isValueChangedFromOriginal(valueInput);
                if (valueCell) {
                    valueCell.dataset.resetPending = "false";
                    valueCell.classList.toggle("metric-cell--dirty", changedFromOriginal);
                }
                const unitSelect = valueCell ? valueCell.querySelector("[data-source-unit-select]") : null;
                if (unitSelect) {
                    const originalUnit = unitSelect.getAttribute("data-original-unit") || "";
                    const currentUnit = unitSelect.dataset.currentUnit || unitSelect.value || originalUnit;
                    if (unitSelect.dataset.resetPending === "true") {
                        unitSelect.dataset.savedUnit = originalUnit;
                        unitSelect.value = originalUnit;
                        unitSelect.dataset.currentUnit = originalUnit;
                    } else if (unitSelect.dataset.dirty === "true") {
                        unitSelect.dataset.savedUnit = currentUnit;
                    }
                    unitSelect.dataset.dirty = "false";
                    unitSelect.dataset.resetPending = "false";
                    updateUnitResetVisibility(unitSelect);
                }
                updateValueResetVisibility(valueInput);
            });
            row.dataset.valueChanged = rowHasChangedMetricValue(row) ? "true" : "false";
            row.dataset.unitChanged = Array.from(row.querySelectorAll("[data-source-unit-select]"))
                .some((select) => isUnitChangedFromOriginal(select)) ? "true" : "false";
            const picker = row.querySelector("[data-mapping-picker]");
            const mappingInput = row.querySelector("[data-standard-term-input]");
            if (picker) {
                const originalCode = picker.getAttribute("data-original-code") || "";
                const originalName = picker.getAttribute("data-original-name") || "";
                if (picker.dataset.resetPending === "true") {
                    picker.dataset.savedCode = originalCode;
                    picker.dataset.savedName = originalName;
                } else if (picker.dataset.dirty === "true") {
                    picker.dataset.savedCode = picker.dataset.currentCode || "";
                    picker.dataset.savedName = picker.dataset.currentName || "";
                }
                picker.dataset.dirty = "false";
                picker.dataset.resetPending = "false";
                const mappingChangedFromOriginal = isMappingChangedFromOriginal(picker);
                picker.classList.toggle("mapping-row--dirty", mappingChangedFromOriginal);
                row.dataset.mappingChanged = mappingChangedFromOriginal ? "true" : "false";
                if (mappingInput) {
                    updateMappingResetVisibility(mappingInput);
                }
            }
            updateUnifiedStatus(row);
        });
        if (status) {
            const precisionWarnings = Number(payload.precision_warnings_total || 0);
            if (precisionWarnings > 0) {
                status.textContent = "已保存，数值已按金额保留两位";
            } else if (payload.combined_workbook_refreshed) {
                status.textContent = "已保存，下载版已刷新";
            } else {
                status.textContent = "已保存";
            }
        }
        return payload;
    } catch (error) {
        if (status) {
            status.textContent = error.message || "保存失败";
        }
        return null;
    } finally {
        if (button) {
            button.disabled = false;
        }
    }
}

async function openUnifiedDownloadPreview(root, link) {
    const href = link.getAttribute("href") || root.getAttribute("data-download-preview-url") || "";
    if (!href) {
        return;
    }
    const edits = collectUnifiedEdits(root);
    if (!edits.length) {
        window.open(href, "_blank", "noopener");
        return;
    }
    const previewWindow = window.open("", "_blank", "noopener");
    const payload = await saveUnifiedEdits(root, { edits });
    if (payload && payload.pass) {
        if (previewWindow) {
            previewWindow.location.href = href;
        } else {
            window.location.href = href;
        }
        return;
    }
    if (previewWindow) {
        previewWindow.close();
    }
}

function initUnifiedProofreadWorkbench() {
    const root = document.querySelector("[data-unified-proofread-workbench]");
    if (!root) {
        return;
    }
    const rows = Array.from(root.querySelectorAll("[data-unified-row]"));
    const tableBody = root.querySelector("[data-unified-table] tbody");
    if (tableBody) {
        Array.from(tableBody.children).forEach((child, index) => {
            child.dataset.originalDomOrder = String(index);
        });
    }
    rows.forEach((row) => {
        row.addEventListener("click", (event) => selectUnifiedRow(row, event.target));
        const dateInput = row.querySelector("[data-metric-date-input]");
        if (dateInput) {
            dateInput.addEventListener("focus", () => {
                selectUnifiedRow(row, dateInput);
                updateDateResetVisibility(dateInput);
            });
            dateInput.addEventListener("input", () => markUnifiedDateChanged(row, dateInput));
            dateInput.addEventListener("blur", () => window.setTimeout(() => updateDateResetVisibility(dateInput), 80));
            const dateReset = dateInput.closest("[data-date-cell]")?.querySelector("[data-reset-date]");
            if (dateReset) {
                dateReset.addEventListener("mousedown", (event) => {
                    event.preventDefault();
                    event.stopPropagation();
                });
                dateReset.addEventListener("click", (event) => {
                    event.stopPropagation();
                    resetUnifiedDate(row, dateInput);
                });
            }
        }
        row.querySelectorAll("[data-metric-value-input]").forEach((valueInput) => {
            valueInput.value = formatMetricNumber(valueInput.value, valueInput.getAttribute("data-value-type") || "");
            valueInput.addEventListener("focus", () => {
                selectUnifiedRow(row, valueInput);
                updateValueResetVisibility(valueInput);
            });
            valueInput.addEventListener("input", () => markUnifiedValueChanged(row, valueInput));
            valueInput.addEventListener("blur", () => {
                const parsed = parseMetricNumberInput(valueInput.value, valueInput.getAttribute("data-value-type") || "");
                if (parsed.valid) {
                    valueInput.dataset.currentValue = parsed.value;
                    valueInput.value = formatMetricNumber(parsed.value, valueInput.getAttribute("data-value-type") || "");
                }
                updateValuePrecisionNote(valueInput, parsed.valid ? parsed : null);
                window.setTimeout(() => updateValueResetVisibility(valueInput), 80);
            });
            const reset = valueInput.closest("[data-value-cell]")?.querySelector("[data-reset-value]");
            if (reset) {
                reset.addEventListener("mousedown", (event) => {
                    event.preventDefault();
                    event.stopPropagation();
                });
                reset.addEventListener("click", (event) => {
                    event.stopPropagation();
                    resetUnifiedValue(row, valueInput);
                });
            }
            const unitSelect = valueInput.closest("[data-value-cell]")?.querySelector("[data-source-unit-select]");
            if (unitSelect) {
                unitSelect.addEventListener("change", () => markUnifiedUnitChanged(row, unitSelect));
                const unitReset = unitSelect.closest("[data-value-cell]")?.querySelector("[data-reset-unit]");
                if (unitReset) {
                    unitReset.addEventListener("click", (event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        resetUnifiedUnit(row, unitSelect);
                    });
                }
            }
        });
        const standardInput = row.querySelector("[data-standard-term-input]");
        if (standardInput) {
            initStandardTermAutocomplete(standardInput, row, root);
        }
        row.querySelectorAll("[data-mapping-decision-form]").forEach((form) => {
            form.addEventListener("submit", () => syncMappingDecisionForm(form));
        });
        const mappingReset = row.querySelector("[data-reset-mapping]");
        if (mappingReset) {
            mappingReset.addEventListener("mousedown", (event) => {
                event.preventDefault();
                event.stopPropagation();
            });
            mappingReset.addEventListener("click", (event) => {
                event.stopPropagation();
                resetUnifiedMapping(row);
            });
        }
    });
    const confidenceToggle = root.querySelector("[data-confidence-toggle]");
    if (confidenceToggle) {
        confidenceToggle.addEventListener("click", () => {
            const texts = root.querySelectorAll("[data-ocr-confidence-text]");
            const shouldShow = Array.from(texts).some((text) => text.hidden);
            texts.forEach((text) => {
                text.hidden = !shouldShow;
            });
            confidenceToggle.classList.toggle("confidence-switch--on", shouldShow);
            confidenceToggle.setAttribute("aria-checked", shouldShow ? "true" : "false");
            confidenceToggle.setAttribute("aria-label", shouldShow ? "隐藏OCR置信度" : "显示OCR置信度");
            const label = confidenceToggle.querySelector("[data-confidence-toggle-label]");
            if (label) {
                label.textContent = shouldShow ? "隐藏OCR置信度" : "显示OCR置信度";
            }
        });
    }
    const searchInput = root.querySelector("[data-unified-table-search]");
    if (searchInput) {
        searchInput.addEventListener("input", () => filterUnifiedRows(root));
    }
    root.querySelectorAll("[data-unified-status-filter], [data-unified-confidence-filter], [data-unified-confidence-threshold], [data-unified-sort]").forEach((control) => {
        control.addEventListener("change", () => filterUnifiedRows(root));
        control.addEventListener("input", () => filterUnifiedRows(root));
    });
    const saveButton = root.querySelector("[data-unified-save]");
    if (saveButton) {
        saveButton.addEventListener("click", () => saveUnifiedEdits(root));
    }
    const previewLink = root.querySelector("[data-unified-download-preview]");
    if (previewLink) {
        previewLink.addEventListener("click", (event) => {
            event.preventDefault();
            openUnifiedDownloadPreview(root, previewLink);
        });
    }
    const accountingForm = root.querySelector("[data-accounting-generate-form]");
    if (accountingForm) {
        accountingForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            const payload = await saveUnifiedEdits(root);
            if (payload && payload.pass) {
                HTMLFormElement.prototype.submit.call(accountingForm);
            }
        });
    }
    initBulkConfidenceControls(root);
    const initiallySelected = root.querySelector(".unified-row--selected") || rows[0];
    if (initiallySelected) {
        selectUnifiedRow(initiallySelected, initiallySelected);
    }
    document.addEventListener("click", (event) => {
        if (!event.target.closest(".standard-term-picker")) {
            closeAutocompleteResults(root);
        }
    });
}

function initWorkbookPreview() {
    const tabs = Array.from(document.querySelectorAll("[data-workbook-preview-tab]"));
    const sheets = Array.from(document.querySelectorAll("[data-workbook-preview-sheet]"));
    if (!tabs.length || !sheets.length) {
        return;
    }
    tabs.forEach((tab) => {
        tab.addEventListener("click", () => {
            const selected = tab.getAttribute("data-workbook-preview-tab") || "0";
            tabs.forEach((candidate) => {
                const active = candidate.getAttribute("data-workbook-preview-tab") === selected;
                candidate.classList.toggle("button--active", active);
                candidate.classList.toggle("button--secondary", !active);
            });
            sheets.forEach((sheet) => {
                sheet.hidden = sheet.getAttribute("data-workbook-preview-sheet") !== selected;
            });
        });
    });
}

function batchUploadErrorMessage(payload, fallback) {
    if (payload && typeof payload.detail === "string") {
        return payload.detail;
    }
    if (payload && typeof payload.message === "string") {
        return payload.message;
    }
    return fallback;
}

async function postBatchForm(url, formData, retryCount = 0) {
    let lastError = null;
    for (let attempt = 0; attempt <= retryCount; attempt += 1) {
        try {
            const response = await fetch(url, {
                method: "POST",
                body: formData,
                credentials: "same-origin",
            });
            let payload = {};
            try {
                payload = await response.json();
            } catch (error) {
                payload = {};
            }
            if (!response.ok) {
                throw new Error(batchUploadErrorMessage(payload, `请求失败（${response.status}）`));
            }
            return payload;
        } catch (error) {
            lastError = error;
            if (attempt < retryCount) {
                await new Promise((resolve) => window.setTimeout(resolve, 700));
            }
        }
    }
    throw lastError || new Error("上传失败");
}

function initDocumentBatchUpload() {
    const form = document.querySelector("[data-document-batch-upload]");
    if (!form || !window.fetch || !window.FormData) {
        return;
    }
    const input = form.querySelector('input[type="file"]');
    const submit = form.querySelector("[data-batch-upload-submit]");
    const reset = form.querySelector("[data-batch-upload-reset]");
    const errorPanel = form.querySelector("[data-batch-upload-error]");
    const progress = form.querySelector("[data-batch-upload-progress]");
    const summary = form.querySelector("[data-batch-upload-summary]");
    const percent = form.querySelector("[data-batch-upload-percent]");
    const bar = form.querySelector("[data-batch-upload-bar]");
    const list = form.querySelector("[data-batch-upload-files]");
    const maxFiles = Number(form.dataset.maxFiles || "5");
    const maxBytes = Number(form.dataset.maxBytes || "0");
    const createBatchUrl = String(form.dataset.createBatchUrl || "").replace(/\/+$/, "");
    const detailBase = String(form.dataset.batchDetailBase || "").replace(/\/+$/, "");
    const storageKey = `autofinance:document-batch-upload:${createBatchUrl}`;
    let activeBatch = null;
    let activeFileSignature = "";
    const uploadedIndexes = new Set();
    const uploadedNames = new Map();

    function showError(message) {
        if (errorPanel) {
            errorPanel.textContent = message;
            errorPanel.hidden = false;
        }
    }

    function setProgress(done, total, label) {
        const value = total ? Math.round((done / total) * 100) : 0;
        if (summary) {
            summary.textContent = label;
        }
        if (percent) {
            percent.textContent = `${value}%`;
        }
        if (bar) {
            bar.style.width = `${value}%`;
        }
    }

    function batchDescriptor(batchId, expectedFiles) {
        const encodedId = window.encodeURIComponent(String(batchId || ""));
        return {
            batch_id: String(batchId || ""),
            expected_files: Number(expectedFiles || 0),
            upload_url: `${createBatchUrl}/${encodedId}/files`,
            queue_url: `${createBatchUrl}/${encodedId}/queue`,
            status_url: `${createBatchUrl}/${encodedId}`,
            detail_url: `${detailBase}/${encodedId}`,
        };
    }

    function readStoredRecovery() {
        try {
            const parsed = JSON.parse(window.localStorage.getItem(storageKey) || "{}");
            return parsed && typeof parsed === "object" ? parsed : {};
        } catch (error) {
            return {};
        }
    }

    function persistRecovery() {
        if (!activeBatch?.batch_id) {
            return;
        }
        try {
            window.localStorage.setItem(storageKey, JSON.stringify({
                batch_id: activeBatch.batch_id,
                expected_files: activeBatch.expected_files,
                file_signature: activeFileSignature,
            }));
        } catch (error) {
            // Server-side batch state still allows recovery from the batch detail page.
        }
    }

    function clearRecovery() {
        try {
            window.localStorage.removeItem(storageKey);
        } catch (error) {
            // Ignore unavailable browser storage.
        }
    }

    async function restoreBatch() {
        const stored = readStoredRecovery();
        const requestedBatchId = String(form.dataset.resumeBatchId || stored.batch_id || "").trim();
        const requestedExpectedFiles = Number(
            form.dataset.resumeExpectedFiles || stored.expected_files || 0
        );
        if (!requestedBatchId) {
            return;
        }
        const candidate = batchDescriptor(requestedBatchId, requestedExpectedFiles);
        try {
            const response = await fetch(candidate.status_url, {
                credentials: "same-origin",
                cache: "no-store",
            });
            if (!response.ok) {
                if (response.status === 404) {
                    clearRecovery();
                }
                return;
            }
            const payload = await response.json();
            if (payload.status !== "uploading") {
                clearRecovery();
                return;
            }
            activeBatch = batchDescriptor(payload.batch_id, payload.expected_files);
            activeFileSignature = (
                stored.batch_id === payload.batch_id
                    ? String(stored.file_signature || "")
                    : ""
            );
            uploadedIndexes.clear();
            uploadedNames.clear();
            (payload.items || []).forEach((item) => {
                const index = Number(item.upload_index);
                if (Number.isInteger(index)) {
                    uploadedIndexes.add(index);
                    uploadedNames.set(index, String(item.original_filename || ""));
                }
            });
            persistRecovery();
            if (progress) {
                progress.hidden = false;
            }
            setProgress(
                uploadedIndexes.size,
                activeBatch.expected_files + 1,
                `已恢复批次，已上传 ${uploadedIndexes.size}/${activeBatch.expected_files} 份`
            );
            if (submit) {
                submit.textContent = "继续上传并开始处理";
            }
            if (reset) {
                reset.hidden = false;
            }
        } catch (error) {
            showError("暂时无法读取未完成批次，请稍后重试。");
        }
    }

    const restorePromise = restoreBatch();

    reset?.addEventListener("click", () => {
        clearRecovery();
        window.location.assign(form.getAttribute("action") || window.location.pathname);
    });

    form.addEventListener("submit", async (event) => {
        event.preventDefault();
        await restorePromise;
        const files = Array.from(input?.files || []);
        const fileSignature = files.map((file) => `${file.name}:${file.size}`).join("|");
        if (!files.length) {
            showError("请选择 PDF 文件。");
            return;
        }
        if (activeBatch && files.length !== activeBatch.expected_files) {
            showError(`恢复该批次时，请重新选择原来的 ${activeBatch.expected_files} 份文件。`);
            return;
        }
        if (activeBatch && activeFileSignature && fileSignature !== activeFileSignature) {
            showError("当前批次已有文件上传成功，请恢复原文件选择后重试。");
            return;
        }
        const mismatchedUploadedFile = files.find((file, index) => (
            uploadedNames.has(index) && uploadedNames.get(index) !== file.name
        ));
        if (mismatchedUploadedFile) {
            showError("所选文件与已上传批次的文件名或顺序不一致。");
            return;
        }
        if (files.length > maxFiles) {
            showError(`每批最多上传 ${maxFiles} 个 PDF 文件。`);
            return;
        }
        const invalidExtension = files.find((file) => !file.name.toLowerCase().endsWith(".pdf"));
        if (invalidExtension) {
            showError(`只支持 PDF 文件：${invalidExtension.name}`);
            return;
        }
        const oversized = maxBytes > 0 ? files.find((file) => file.size > maxBytes) : null;
        if (oversized) {
            showError(`${oversized.name} 超过单文件大小限制。`);
            return;
        }

        if (errorPanel) {
            errorPanel.hidden = true;
            errorPanel.textContent = "";
        }
        if (progress) {
            progress.hidden = false;
        }
        if (submit) {
            submit.disabled = true;
            submit.textContent = "正在上传…";
        }
        if (list) {
            list.replaceChildren();
            files.forEach((file) => {
                const item = document.createElement("li");
                item.textContent = `${file.name} — 等待上传`;
                list.appendChild(item);
            });
        }

        try {
            if (!activeBatch) {
                const createData = new FormData();
                createData.append("expected_files", String(files.length));
                const createdBatch = await postBatchForm(createBatchUrl, createData);
                activeBatch = batchDescriptor(
                    createdBatch.batch_id,
                    createdBatch.expected_files
                );
            }
            activeFileSignature = activeFileSignature || fileSignature;
            persistRecovery();
            for (let index = 0; index < files.length; index += 1) {
                const file = files[index];
                const row = list?.children[index];
                if (uploadedIndexes.has(index)) {
                    if (row) {
                        row.textContent = `${file.name} — 已上传`;
                    }
                    continue;
                }
                if (row) {
                    row.textContent = `${file.name} — 正在上传`;
                }
                setProgress(
                    uploadedIndexes.size,
                    files.length + 1,
                    `正在上传第 ${index + 1}/${files.length} 份`
                );
                const uploadData = new FormData();
                uploadData.append("upload_index", String(index));
                uploadData.append("uploaded_file", file, file.name);
                await postBatchForm(activeBatch.upload_url, uploadData, 1);
                uploadedIndexes.add(index);
                uploadedNames.set(index, file.name);
                persistRecovery();
                if (row) {
                    row.textContent = `${file.name} — 已上传`;
                }
            }
            setProgress(files.length, files.length + 1, "正在加入处理队列");
            await postBatchForm(activeBatch.queue_url, new FormData());
            setProgress(files.length + 1, files.length + 1, "上传完成，正在打开批次状态");
            clearRecovery();
            window.location.assign(activeBatch.detail_url);
        } catch (error) {
            showError(error instanceof Error ? error.message : "上传失败，请重试。");
            if (submit) {
                submit.disabled = false;
                submit.textContent = "重试上传";
            }
        }
    });
}

function initDocumentBatchStatus() {
    const root = document.querySelector("[data-document-batch-status]");
    if (!root || root.dataset.active !== "true") {
        return;
    }
    const poll = async () => {
        try {
            const response = await fetch(root.dataset.statusUrl, {
                credentials: "same-origin",
                cache: "no-store",
            });
            if (response.ok) {
                const payload = await response.json();
                const token = payload.state_token || "";
                if (token !== root.dataset.stateToken) {
                    window.location.reload();
                    return;
                }
                if (payload.active) {
                    window.setTimeout(poll, 2500);
                }
                return;
            }
        } catch (error) {
            // A temporary network failure should not turn a running batch into an error.
        }
        window.setTimeout(poll, 4000);
    };
    window.setTimeout(poll, 1800);
}

window.formatMetricNumber = formatMetricNumber;
window.parseMetricNumberInput = parseMetricNumberInput;

document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll('input[name="mode"]').forEach((input) => {
        input.addEventListener("change", toggleModePanels);
    });
    toggleModePanels();
    initRawReviewSheet();
    initMappingReviewSheet();
    initUnifiedProofreadWorkbench();
    initWorkbookPreview();
    initDocumentBatchUpload();
    initDocumentBatchStatus();
});
