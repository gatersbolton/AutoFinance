from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from playwright.sync_api import sync_playwright

from project_paths import WEB_GENERATED_ROOT
from webapp.config import load_settings
from webapp.document_library import document_to_job, load_document
from webapp.unified_review import load_unified_review_items, unified_review_dir


SUMMARY_PATH = WEB_GENERATED_ROOT / "unified_proofread_summary.json"


def main() -> int:
    summary: dict[str, object] = {
        "pass": False,
        "doc_id": "D01",
        "page_url": "",
        "rows_total": 0,
        "numeric_formatting_pass": False,
        "confidence_toggle_pass": False,
        "autocomplete_tests": [],
        "mapping_edit_saved": False,
        "numeric_edit_saved": False,
        "reset_button_present": False,
        "status_labels_pass": False,
        "source_highlight_pass": False,
        "old_routes_non_regression_pass": False,
        "path_hygiene_pass": False,
        "output_files": [],
    }
    process: subprocess.Popen[str] | None = None
    try:
        settings = load_settings()
        settings.ensure_directories()
        document = load_document(settings, "D01")
        job = document_to_job(settings, document)
        items = load_unified_review_items(job)
        if not items:
            raise RuntimeError("D01 has no unified proofreading rows.")
        target = _choose_target(items)
        target_id = str(target.get("review_item_id", ""))
        target_raw_id = str(target.get("raw_metric_id", ""))
        summary["rows_total"] = len(items)

        action_dir = unified_review_dir(job)
        action_json = action_dir / "unified_review_actions.json"
        before_actions = _read_actions(action_json)

        port = _free_port()
        base_url = f"http://127.0.0.1:{port}"
        page_url = f"{base_url}/documents/D01/proofread"
        summary["page_url"] = page_url
        process = _start_server(port)
        _wait_for_server(f"{base_url}/healthz")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1500, "height": 980})
                response = page.goto(page_url, wait_until="networkidle")
                if response is None or response.status != 200:
                    raise RuntimeError(f"proofread page returned {response.status if response else 'no response'}")
                if page.locator("[data-unified-proofread-workbench]").count() != 1:
                    raise RuntimeError("unified proofreading workbench was not rendered.")
                for header in ("原始术语", "指标数值", "标准术语", "状态"):
                    if page.get_by_text(header, exact=True).count() < 1:
                        raise RuntimeError(f"missing table header: {header}")

                formatted_values = page.locator("[data-metric-value-input]").evaluate_all(
                    "nodes => nodes.map(node => node.value)"
                )
                visible_cell_errors = page.locator(".cell-error").evaluate_all(
                    "nodes => nodes.filter(node => !!(node.offsetWidth || node.offsetHeight || node.getClientRects().length)).length"
                )
                summary["numeric_formatting_pass"] = (
                    "396,149,420.62" in formatted_values
                    or any("," in value and value.replace(",", "").replace(".", "").isdigit() for value in formatted_values)
                ) and visible_cell_errors == 0

                confidence_cells = page.locator("[data-confidence-text]")
                hidden_before = confidence_cells.nth(0).evaluate("node => node.hidden")
                page.locator("[data-confidence-toggle]").click()
                page.wait_for_timeout(200)
                visible_after = not confidence_cells.nth(0).evaluate("node => node.hidden")
                switch_on = page.locator("[data-confidence-toggle]").get_attribute("aria-checked") == "true"
                has_confidence_text = page.get_by_text("阿里云", exact=False).count() >= 1 or page.get_by_text("未记录", exact=True).count() >= 1
                page.locator("[data-confidence-toggle]").click()
                summary["confidence_toggle_pass"] = bool(hidden_before and visible_after and switch_on and has_confidence_text)

                row = page.locator(f'[data-review-item-id="{target_id}"]').first
                row.scroll_into_view_if_needed()
                row.locator("[data-unified-term-cell]").click()
                page.wait_for_timeout(300)
                highlight_text = page.locator("[data-highlight-status]").first.text_content(timeout=5000) or ""
                summary["source_highlight_pass"] = "已高亮" in highlight_text or "未记录位置" in highlight_text

                input_box = row.locator("[data-standard-term-input]").first
                value_input = row.locator("[data-metric-value-input]").first
                current_code = row.locator("[data-mapping-picker]").first.get_attribute("data-current-code") or ""
                current_value = value_input.get_attribute("data-current-value") or ""
                if current_code == "ZT_002" or current_value == "12345.67":
                    if current_code == "ZT_002":
                        input_box.focus()
                        page.wait_for_timeout(120)
                        row.locator("[data-reset-mapping]").first.click()
                    if current_value == "12345.67":
                        value_input.focus()
                        page.wait_for_timeout(120)
                        row.locator("[data-reset-value]").first.click()
                    page.locator("[data-unified-save]").click()
                    page.locator("[data-unified-save-status]", has_text="已保存").wait_for(timeout=5000)
                    page.goto(page_url, wait_until="networkidle")
                    row = page.locator(f'[data-review-item-id="{target_id}"]').first
                    row.scroll_into_view_if_needed()
                    input_box = row.locator("[data-standard-term-input]").first
                    value_input = row.locator("[data-metric-value-input]").first

                empty_before = page.locator(".autocomplete-empty").count()
                autocomplete_results = []
                for query, expected in (
                    ("2", "ZT_002 短期借款"),
                    ("短期", "ZT_002 短期借款"),
                    ("dqjk", "ZT_002 短期借款"),
                    ("应付", "ZT_003 应付票据"),
                ):
                    input_box.fill(query)
                    option = page.locator(".autocomplete-option", has_text=expected).first
                    option.wait_for(state="visible", timeout=5000)
                    autocomplete_results.append({"query": query, "expected": expected, "found": True})
                    input_box.press("Escape")
                    page.wait_for_timeout(120)
                input_box.fill("not-a-standard-term")
                page.locator(".autocomplete-empty", has_text="没有找到标准术语").first.wait_for(state="visible", timeout=5000)
                input_box.press("Escape")
                page.wait_for_timeout(120)
                escape_closed = page.locator(".autocomplete-empty").count() == 0
                input_box.fill("2")
                page.locator(".autocomplete-option", has_text="ZT_002 短期借款").first.wait_for(state="visible", timeout=5000)
                page.locator(".source-panel").click()
                page.wait_for_timeout(120)
                outside_closed = page.locator(".autocomplete-option", has_text="ZT_002 短期借款").count() == 0
                input_box.fill("2")
                option = page.locator(".autocomplete-option", has_text="ZT_002 短期借款").first
                option.wait_for(state="visible", timeout=5000)
                option.click()
                page.wait_for_timeout(150)
                select_closed = page.locator(".autocomplete-option", has_text="ZT_002 短期借款").count() == 0
                input_box.fill("应付")
                payable_option = page.locator(".autocomplete-option", has_text="ZT_003 应付票据").first
                payable_option.wait_for(state="visible", timeout=5000)
                payable_option.click()
                page.wait_for_timeout(150)
                payable_click_selected = "ZT_003 应付票据" in (input_box.input_value() or "")
                input_box.fill("")
                page.wait_for_timeout(150)
                clear_closed = page.locator(".autocomplete-empty").count() == 0 and page.locator(".autocomplete-option").count() == 0
                summary["autocomplete_tests"] = autocomplete_results + [
                    {"query": "", "no_results_hidden": empty_before == 0},
                    {"query": "not-a-standard-term", "no_results_after_non_empty": escape_closed},
                    {"event": "outside_click", "closed": outside_closed},
                    {"event": "select_result", "closed": select_closed},
                    {"query": "应付", "expected": "ZT_003 应付票据", "click_selected": payable_click_selected},
                    {"event": "clear_input", "closed": clear_closed},
                ]

                input_box.fill("2")
                page.locator(".autocomplete-option", has_text="ZT_002 短期借款").first.click()
                page.wait_for_timeout(150)
                mapping_reset_visible_focused = row.locator("[data-reset-mapping]").first.is_visible()
                page.locator(".source-panel").click()
                page.wait_for_timeout(160)
                mapping_reset_hidden_blurred = not row.locator("[data-reset-mapping]").first.is_visible()
                input_box.focus()
                page.wait_for_timeout(120)
                mapping_reset_visible_refocused = row.locator("[data-reset-mapping]").first.is_visible()
                value_input.fill("12,345.67")
                page.wait_for_timeout(120)
                value_reset_visible_focused = row.locator("[data-reset-value]").first.is_visible()
                page.locator(".source-panel").click()
                page.wait_for_timeout(160)
                value_reset_hidden_blurred = not row.locator("[data-reset-value]").first.is_visible()
                value_input.focus()
                page.wait_for_timeout(120)
                value_reset_visible_refocused = row.locator("[data-reset-value]").first.is_visible()
                summary["reset_button_present"] = bool(
                    mapping_reset_visible_focused
                    and mapping_reset_hidden_blurred
                    and mapping_reset_visible_refocused
                    and value_reset_visible_focused
                    and value_reset_hidden_blurred
                    and value_reset_visible_refocused
                )

                page.locator("[data-unified-save]").click()
                page.locator("[data-unified-save-status]", has_text="已保存").wait_for(timeout=5000)
                status_labels = row.locator("[data-status-badge]").evaluate_all("nodes => nodes.map(node => node.textContent.trim())")
                summary["status_labels_pass"] = bool("数值已修改" in status_labels and "术语已修改" in status_labels and "已修改" not in status_labels)
                reset_hidden_after_save = not row.locator("[data-reset-value]").first.is_visible() and not row.locator("[data-reset-mapping]").first.is_visible()
                value_input.focus()
                page.wait_for_timeout(120)
                value_reset_visible_after_save_focus = row.locator("[data-reset-value]").first.is_visible()
                input_box.focus()
                page.wait_for_timeout(120)
                mapping_reset_visible_after_save_focus = row.locator("[data-reset-mapping]").first.is_visible()
                summary["reset_button_present"] = bool(
                    summary["reset_button_present"]
                    and reset_hidden_after_save
                    and value_reset_visible_after_save_focus
                    and mapping_reset_visible_after_save_focus
                )

                raw_response = page.goto(f"{base_url}/documents/D01/raw-review", wait_until="networkidle")
                raw_ok = raw_response is not None and raw_response.status == 200 and page.locator("[data-raw-review-workbench]").count() == 1
                mapping_response = page.goto(f"{base_url}/documents/D01/mapping-review", wait_until="networkidle")
                mapping_ok = mapping_response is not None and mapping_response.status == 200 and page.locator("[data-mapping-review-workbench]").count() == 1
                summary["old_routes_non_regression_pass"] = bool(raw_ok and mapping_ok)
            finally:
                browser.close()

        after_actions = _read_actions(action_json)
        new_actions = after_actions[len(before_actions) :]
        summary["mapping_edit_saved"] = any(
            action.get("raw_metric_id") == target_raw_id
            and action.get("edit_type") == "mapping_change"
            and action.get("new_code") == "ZT_002"
            and action.get("new_name") == "短期借款"
            for action in new_actions
        )
        summary["numeric_edit_saved"] = any(
            action.get("raw_metric_id") == target_raw_id and action.get("edit_type") == "value_change" and action.get("new_value") == "12345.67"
            for action in new_actions
        )
        output_files = [
            action_dir / "unified_review_actions.csv",
            action_dir / "unified_review_actions.json",
            action_dir / "unified_review_summary.json",
            Path(job.result_dir) / "unified_review" / "unified_review_summary.json",
            SUMMARY_PATH,
        ]
        summary["output_files"] = [str(path) for path in output_files if path.exists() or path == SUMMARY_PATH]
        summary["path_hygiene_pass"] = all(str(path.resolve()).startswith(str(WEB_GENERATED_ROOT.resolve())) for path in output_files)
        summary["pass"] = bool(
            summary["rows_total"]
            and summary["numeric_formatting_pass"]
            and summary["confidence_toggle_pass"]
            and summary["autocomplete_tests"]
            and all(
                item.get("found", True)
                and item.get("closed", True)
                and item.get("click_selected", True)
                and item.get("no_results_hidden", True)
                and item.get("no_results_after_non_empty", True)
                for item in summary["autocomplete_tests"]
            )
            and summary["mapping_edit_saved"]
            and summary["numeric_edit_saved"]
            and summary["reset_button_present"]
            and summary["status_labels_pass"]
            and summary["source_highlight_pass"]
            and summary["old_routes_non_regression_pass"]
            and summary["path_hygiene_pass"]
        )
    except Exception as exc:
        summary["error"] = str(exc)
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
        SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
        SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("pass") else 1


def _choose_target(items: list[dict[str, object]]) -> dict[str, object]:
    for item in items:
        if item.get("original_metric_name") == "货币资金" and item.get("original_code") == "ZT_001" and item.get("current_code") != "ZT_002":
            return item
    for item in items:
        if item.get("original_metric_name") == "货币资金" and item.get("original_code") == "ZT_001":
            return item
    return items[0]


def _start_server(port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["WEBAPP_AUTH_REQUIRED"] = "0"
    env["WEBAPP_ENABLE_LOCAL_WORKER"] = "0"
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "webapp.main:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_for_server(url: str) -> None:
    deadline = time.time() + 30
    last_error = ""
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last_error = str(exc)
            time.sleep(0.25)
    raise RuntimeError(f"web server did not start: {last_error}")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_actions(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


if __name__ == "__main__":
    raise SystemExit(main())
