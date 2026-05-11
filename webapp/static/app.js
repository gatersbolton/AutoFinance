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
    const normalizedType = String(valueType || "").toLowerCase();
    if (text.includes("%") || ["ratio", "percentage", "percent"].includes(normalizedType)) {
        return text;
    }
    const normalized = text.replace(/[,，]/g, "");
    const match = normalized.match(/^([+-]?)(?:(\d+)(?:\.(\d*))?|\.(\d+))$/);
    if (!match) {
        return text;
    }
    const sign = match[1] || "";
    const intPart = (match[2] || "0").replace(/^0+(?=\d)/, "") || "0";
    const fracPart = match[3] !== undefined ? match[3] : match[4];
    const grouped = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    if (fracPart === undefined) {
        return `${sign}${grouped}`;
    }
    return `${sign}${grouped}.${fracPart}`;
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
        return { valid: true, value: `${sign}0.${match[4]}`, reason: "" };
    }
    const intPart = (match[2] || "0").replace(/^0+(?=\d)/, "") || "0";
    if (match[3] === undefined) {
        return { valid: true, value: `${sign}${intPart}`, reason: "" };
    }
    return { valid: true, value: `${sign}${intPart}.${match[3]}`, reason: "" };
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
    if (row.dataset.mappingChanged === "true") {
        list.appendChild(statusBadge("term_changed", "术语已修改"));
    }
    if (!list.children.length) {
        list.appendChild(statusBadge(row.dataset.baseStatusCode || "unmapped", row.dataset.baseStatusLabel || "未映射"));
    }
}

function isValueChangedFromOriginal(input) {
    return (input.dataset.currentValue || "") !== (input.getAttribute("data-original-value") || "");
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
    const highlightTarget = target && target.closest("[data-value-cell]") ? target.closest("[data-value-cell]") : row;
    if (image && nextImageUrl && image.getAttribute("src") !== nextImageUrl) {
        image.addEventListener("load", () => highlightSourceForCell(highlightTarget, root), { once: true });
        image.setAttribute("src", nextImageUrl);
        return;
    }
    highlightSourceForCell(highlightTarget, root);
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
    row.dataset.valueChanged = changedFromOriginal ? "true" : "false";
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
    }
    row.dataset.valueChanged = "false";
    updateValueResetVisibility(input);
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
        const valueInput = row.querySelector("[data-metric-value-input]");
        const valueCell = row.querySelector("[data-value-cell]");
        if (valueInput && valueInput.dataset.dirty === "true") {
            const savedValue = valueInput.dataset.savedValue || valueInput.getAttribute("data-original-value") || "";
            edits.push({
                item_id: itemId,
                raw_metric_id: rawMetricId,
                edit_type: "value_change",
                previous_value: savedValue,
                new_value: valueInput.dataset.currentValue || valueInput.value,
            });
        } else if (valueCell && valueCell.dataset.resetPending === "true") {
            const savedValue = valueInput ? (valueInput.dataset.savedValue || valueInput.getAttribute("data-original-value") || "") : "";
            edits.push({
                item_id: itemId,
                raw_metric_id: rawMetricId,
                edit_type: "reset_value",
                previous_value: savedValue,
                new_value: valueInput ? (valueInput.getAttribute("data-original-value") || "") : "",
            });
        }
        const picker = row.querySelector("[data-mapping-picker]");
        if (picker && picker.dataset.dirty === "true") {
            const savedCode = picker.dataset.savedCode || picker.getAttribute("data-original-code") || "";
            const savedName = picker.dataset.savedName || picker.getAttribute("data-original-name") || "";
            edits.push({
                item_id: itemId,
                raw_metric_id: rawMetricId,
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

function filterUnifiedRows(root) {
    const queryInput = root.querySelector("[data-unified-table-search]");
    if (!queryInput) {
        return;
    }
    const query = queryInput.value.trim().toLowerCase();
    const visibleSections = new Set();
    root.querySelectorAll("[data-unified-row]").forEach((row) => {
        const text = row.textContent.toLowerCase();
        const visible = !query || text.includes(query);
        row.hidden = !visible;
        if (visible) {
            visibleSections.add(row.getAttribute("data-section-key") || "");
        }
    });
    root.querySelectorAll("[data-section-header]").forEach((header) => {
        header.hidden = !visibleSections.has(header.getAttribute("data-section-key") || "");
    });
}

async function saveUnifiedEdits(root) {
    const status = root.querySelector("[data-unified-save-status]");
    const button = root.querySelector("[data-unified-save]");
    const edits = collectUnifiedEdits(root);
    if (!edits.length) {
        if (status) {
            status.textContent = "没有需要保存的修改";
        }
        return;
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
        root.querySelectorAll("[data-unified-row]").forEach((row) => {
            const valueInput = row.querySelector("[data-metric-value-input]");
            const valueCell = row.querySelector("[data-value-cell]");
            if (valueInput) {
                const originalValue = valueInput.getAttribute("data-original-value") || "";
                const currentValue = valueInput.dataset.currentValue || originalValue;
                if (valueCell && valueCell.dataset.resetPending === "true") {
                    valueInput.dataset.savedValue = originalValue;
                } else if (valueInput.dataset.dirty === "true") {
                    valueInput.dataset.savedValue = currentValue;
                }
                valueInput.dataset.dirty = "false";
                const changedFromOriginal = isValueChangedFromOriginal(valueInput);
                if (valueCell) {
                    valueCell.dataset.resetPending = "false";
                    valueCell.classList.toggle("metric-cell--dirty", changedFromOriginal);
                }
                row.dataset.valueChanged = changedFromOriginal ? "true" : "false";
                updateValueResetVisibility(valueInput);
            }
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
            status.textContent = "已保存";
        }
    } catch (error) {
        if (status) {
            status.textContent = error.message || "保存失败";
        }
    } finally {
        if (button) {
            button.disabled = false;
        }
    }
}

function initUnifiedProofreadWorkbench() {
    const root = document.querySelector("[data-unified-proofread-workbench]");
    if (!root) {
        return;
    }
    const rows = Array.from(root.querySelectorAll("[data-unified-row]"));
    rows.forEach((row) => {
        row.addEventListener("click", (event) => selectUnifiedRow(row, event.target));
        const valueInput = row.querySelector("[data-metric-value-input]");
        if (valueInput) {
            valueInput.value = formatMetricNumber(valueInput.value, valueInput.getAttribute("data-value-type") || "");
            valueInput.addEventListener("focus", () => {
                selectUnifiedRow(row, valueInput);
                updateValueResetVisibility(valueInput);
            });
            valueInput.addEventListener("input", () => markUnifiedValueChanged(row, valueInput));
            valueInput.addEventListener("blur", () => {
                const parsed = parseMetricNumberInput(valueInput.value, valueInput.getAttribute("data-value-type") || "");
                if (parsed.valid) {
                    valueInput.value = formatMetricNumber(parsed.value, valueInput.getAttribute("data-value-type") || "");
                }
                window.setTimeout(() => updateValueResetVisibility(valueInput), 80);
            });
            const reset = row.querySelector("[data-reset-value]");
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
        }
        const standardInput = row.querySelector("[data-standard-term-input]");
        if (standardInput) {
            initStandardTermAutocomplete(standardInput, row, root);
        }
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
            const texts = root.querySelectorAll("[data-confidence-text]");
            const shouldShow = Array.from(texts).some((text) => text.hidden);
            texts.forEach((text) => {
                text.hidden = !shouldShow;
            });
            confidenceToggle.classList.toggle("confidence-switch--on", shouldShow);
            confidenceToggle.setAttribute("aria-checked", shouldShow ? "true" : "false");
            confidenceToggle.setAttribute("aria-label", shouldShow ? "隐藏置信度" : "显示置信度");
            const label = confidenceToggle.querySelector("[data-confidence-toggle-label]");
            if (label) {
                label.textContent = shouldShow ? "隐藏置信度" : "显示置信度";
            }
        });
    }
    const searchInput = root.querySelector("[data-unified-table-search]");
    if (searchInput) {
        searchInput.addEventListener("input", () => filterUnifiedRows(root));
    }
    const saveButton = root.querySelector("[data-unified-save]");
    if (saveButton) {
        saveButton.addEventListener("click", () => saveUnifiedEdits(root));
    }
    const initiallySelected = root.querySelector(".unified-row--selected") || rows[0];
    const image = root.querySelector("[data-source-page-image]");
    if (image) {
        image.addEventListener("load", () => {
            if (initiallySelected) {
                selectUnifiedRow(initiallySelected, initiallySelected);
            }
        });
    }
    if (initiallySelected) {
        selectUnifiedRow(initiallySelected, initiallySelected);
    }
    document.addEventListener("click", (event) => {
        if (!event.target.closest(".standard-term-picker")) {
            closeAutocompleteResults(root);
        }
    });
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
});
