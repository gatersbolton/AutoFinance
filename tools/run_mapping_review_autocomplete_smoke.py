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
from webapp.simple_flow import load_mapping_review_items, mapping_review_dir


SUMMARY_PATH = WEB_GENERATED_ROOT / "mapping_review_autocomplete_summary.json"


def main() -> int:
    summary: dict[str, object] = {
        "pass": False,
        "doc_id": "D01",
        "page_url": "",
        "rows_total": 0,
        "autocomplete_tests": [],
        "selected_original_metric": "",
        "previous_mapping": {},
        "new_mapping": {},
        "action_saved": False,
        "bbox_highlight_available": False,
        "raw_review_non_regression_pass": False,
        "path_hygiene_pass": False,
    }
    process: subprocess.Popen[str] | None = None
    try:
        settings = load_settings()
        settings.ensure_directories()
        document = load_document(settings, "D01")
        job = document_to_job(settings, document)
        items = load_mapping_review_items(job)
        if not items:
            raise RuntimeError("D01 has no mapping_review_items.")
        target = next((item for item in items if item.get("original_metric_name") == "货币资金"), items[0])
        target_id = str(target.get("review_item_id", ""))
        summary["rows_total"] = len(items)
        summary["selected_original_metric"] = target.get("original_metric_name", "")
        summary["previous_mapping"] = {
            "code": target.get("current_code", ""),
            "name": target.get("current_name", ""),
        }

        action_dir = mapping_review_dir(job)
        action_json = action_dir / "mapping_review_actions.json"
        before_actions = _read_actions(action_json)

        port = _free_port()
        base_url = f"http://127.0.0.1:{port}"
        page_url = f"{base_url}/documents/D01/mapping-review"
        summary["page_url"] = page_url
        process = _start_server(port)
        _wait_for_server(f"{base_url}/healthz")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1440, "height": 960})
                response = page.goto(page_url, wait_until="networkidle")
                if response is None or response.status != 200:
                    raise RuntimeError(f"mapping review page returned {response.status if response else 'no response'}")
                if page.locator("[data-mapping-review-workbench]").count() != 1:
                    raise RuntimeError("mapping review workbench was not rendered.")

                row = page.locator(f'[data-review-item-id="{target_id}"]').first
                row.click()
                page.wait_for_timeout(300)
                bbox = row.get_attribute("data-bbox") or ""
                status_text = page.locator("[data-highlight-status]").first.text_content(timeout=5000) or ""
                summary["bbox_highlight_available"] = bool(bbox.strip()) and "已高亮" in status_text

                input_box = row.locator("[data-standard-term-input]").first
                input_box.fill("2")
                option = page.locator(".autocomplete-option", has_text="ZT_002 短期借款").first
                option.wait_for(state="visible", timeout=5000)
                summary["autocomplete_tests"] = [
                    {
                        "query": "2",
                        "expected": "ZT_002 短期借款",
                        "found": True,
                    }
                ]
                option.click()
                row.locator('button[name="action"][value="change_mapping"]').click()
                page.wait_for_load_state("networkidle")

                raw_response = page.goto(f"{base_url}/documents/D01/raw-review", wait_until="networkidle")
                summary["raw_review_non_regression_pass"] = bool(
                    raw_response is not None
                    and raw_response.status == 200
                    and page.locator("[data-raw-review-workbench]").count() == 1
                )
            finally:
                browser.close()

        after_actions = _read_actions(action_json)
        new_actions = after_actions[len(before_actions) :]
        last_action = new_actions[-1] if new_actions else (after_actions[-1] if after_actions else {})
        summary["new_mapping"] = {
            "code": last_action.get("selected_code", ""),
            "name": last_action.get("selected_name", ""),
        }
        summary["action_saved"] = (
            last_action.get("review_item_id") == target_id
            and last_action.get("action") == "change_mapping"
            and last_action.get("previous_code") == target.get("current_code", "")
            and last_action.get("previous_name") == target.get("current_name", "")
            and last_action.get("selected_code") == "ZT_002"
            and last_action.get("selected_name") == "短期借款"
            and last_action.get("original_metric_name") == target.get("original_metric_name", "")
        )
        summary["path_hygiene_pass"] = str(action_json.resolve()).startswith(str(WEB_GENERATED_ROOT.resolve())) and str(SUMMARY_PATH.resolve()).startswith(
            str(WEB_GENERATED_ROOT.resolve())
        )
        summary["pass"] = bool(
            summary["rows_total"]
            and summary["autocomplete_tests"]
            and summary["action_saved"]
            and summary["bbox_highlight_available"]
            and summary["raw_review_non_regression_pass"]
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


def _start_server(port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["WEBAPP_AUTH_REQUIRED"] = "0"
    env["WEBAPP_ENABLE_LOCAL_WORKER"] = "0"
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "webapp.main:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=Path(__file__).resolve().parents[1],
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
