function toggleModePanels() {
    const checked = document.querySelector('input[name="mode"]:checked');
    const activeMode = checked ? checked.value : "existing_ocr_outputs";
    document.querySelectorAll("[data-mode-panel]").forEach((panel) => {
        panel.hidden = panel.getAttribute("data-mode-panel") !== activeMode;
    });
}

document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll('input[name="mode"]').forEach((input) => {
        input.addEventListener("change", toggleModePanels);
    });
    toggleModePanels();
});
