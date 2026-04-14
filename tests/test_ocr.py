import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import OCR as ocr


class FakeImage:
    def __init__(self, width, height, mode="RGB"):
        self.size = (width, height)
        self.mode = mode

    def convert(self, mode):
        return FakeImage(self.size[0], self.size[1], mode=mode)

    def resize(self, size, resample=None):
        return FakeImage(size[0], size[1], mode=self.mode)


class FakeProvider(ocr.OCRProvider):
    def __init__(self, name, responses, secrets=None):
        self.name = name
        self.responses = responses
        self._secrets = list(secrets or [])

    def secret_values(self):
        return list(self._secrets)

    def recognize_page(self, page):
        response = self.responses[page.page_number]
        if isinstance(response, Exception):
            raise response
        return response


class OCRToolTests(unittest.TestCase):
    def write_secret(self, path):
        path.write_text(
            "\n".join(
                [
                    "Tencent:",
                    "SecretId:tencent-id",
                    "SecretKey:tencent-key",
                    "",
                    "Aliyun:",
                    "AccessKey ID:aliyun-id",
                    "AccessKey Secret:aliyun-secret",
                ]
            ),
            encoding="utf-8",
        )

    def test_parse_secret_file_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            secret_path = Path(tmpdir) / "secret"
            self.write_secret(secret_path)

            sections = ocr.parse_secret_file(secret_path)

            self.assertEqual(sections["tencent"]["secretid"], "tencent-id")
            self.assertEqual(sections["tencent"]["secretkey"], "tencent-key")
            self.assertEqual(sections["aliyun"]["accesskeyid"], "aliyun-id")
            self.assertEqual(sections["aliyun"]["accesskeysecret"], "aliyun-secret")

    def test_parse_secret_file_rejects_unsupported_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            secret_path = Path(tmpdir) / "secret"
            secret_path.write_text("Tencent:\nBadKey:value\n", encoding="utf-8")

            with self.assertRaises(ocr.SecretFormatError):
                ocr.parse_secret_file(secret_path)

    def test_discover_pdf_files_returns_sorted_pdf_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir)
            (input_dir / "b.pdf").write_bytes(b"%PDF-b")
            (input_dir / "a.pdf").write_bytes(b"%PDF-a")
            (input_dir / "notes.txt").write_text("ignore", encoding="utf-8")

            result = ocr.discover_pdf_files(input_dir)

            self.assertEqual([path.name for path in result], ["a.pdf", "b.pdf"])

    def test_optimize_image_for_ocr_resizes_until_under_limit(self):
        def fake_save(image, quality):
            payload_size = int((image.size[0] * image.size[1]) / 200) + quality * 100
            return b"x" * payload_size

        with mock.patch.object(ocr, "save_jpeg_bytes", side_effect=fake_save):
            payload, width, height = ocr.optimize_image_for_ocr(
                image=FakeImage(4000, 3000),
                max_binary_bytes=50_000,
                max_dimension=8192,
            )

        self.assertLess(width, 4000)
        self.assertLess(height, 3000)
        self.assertLessEqual(len(payload), 50_000)

    def test_render_pdf_pages_uses_pdf_renderer_and_optimizer(self):
        class FakePixmap:
            width = 10
            height = 20
            samples = b"0" * 600

        class FakePage:
            def get_pixmap(self, matrix=None, alpha=False):
                return FakePixmap()

        class FakeDocument:
            def __len__(self):
                return 2

            def load_page(self, index):
                return FakePage()

            def close(self):
                return None

        class FakeFitz:
            @staticmethod
            def open(path):
                return FakeDocument()

            @staticmethod
            def Matrix(x, y):
                return (x, y)

        class FakeImageModule:
            @staticmethod
            def frombytes(mode, size, samples):
                return FakeImage(size[0], size[1], mode=mode)

        with mock.patch.object(ocr, "get_fitz_module", return_value=FakeFitz()), mock.patch.object(
            ocr,
            "get_pillow_image_module",
            return_value=FakeImageModule(),
        ), mock.patch.object(
            ocr,
            "optimize_image_for_ocr",
            return_value=(b"jpeg-bytes", 10, 20),
        ):
            pages = ocr.render_pdf_pages(Path("demo.pdf"))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0].page_number, 1)
        self.assertEqual(pages[1].page_number, 2)
        self.assertEqual(pages[0].image_bytes, b"jpeg-bytes")

    def test_normalize_tencent_blocks(self):
        response = {
            "TextDetections": [
                {
                    "DetectedText": "Revenue",
                    "Confidence": 98,
                    "ItemPolygon": {"X": 1, "Y": 2, "Width": 30, "Height": 10},
                    "Polygon": [{"X": 1, "Y": 2}],
                    "AdvancedInfo": '{"paragraph":{"id":1}}',
                }
            ]
        }

        blocks = ocr.normalize_tencent_blocks(response)
        text = ocr.extract_tencent_text(response)

        self.assertEqual(text, "Revenue")
        self.assertEqual(blocks[0]["text"], "Revenue")
        self.assertEqual(blocks[0]["advanced_info"]["paragraph"]["id"], 1)

    def test_normalize_tencent_table_blocks(self):
        response = {
            "TableDetections": [
                {
                    "Type": 1,
                    "TableCoordPoint": [{"X": 0, "Y": 0}],
                    "Cells": [
                        {
                            "Text": "资产负债表",
                            "Confidence": 99,
                            "Type": "body",
                            "ColTl": 0,
                            "RowTl": 0,
                            "ColBr": 1,
                            "RowBr": 0,
                            "Polygon": [{"X": 1, "Y": 2}],
                        }
                    ],
                }
            ]
        }

        text = ocr.extract_tencent_table_text(response)
        blocks = ocr.normalize_tencent_table_blocks(response)

        self.assertEqual(text, "资产负债表")
        self.assertEqual(blocks[0]["text"], "资产负债表")
        self.assertEqual(blocks[0]["cell_range"]["col_tl"], 0)
        self.assertEqual(blocks[0]["table_type"], 1)

    def test_normalize_aliyun_blocks(self):
        raw = {
            "Data": json.dumps(
                {
                    "content": "Total Assets",
                    "prism_wordsInfo": [
                        {
                            "word": "Total",
                            "prob": 99,
                            "x": 1,
                            "y": 2,
                            "width": 3,
                            "height": 4,
                            "pos": [{"x": 1, "y": 2}],
                        }
                    ],
                }
            )
        }

        data = ocr.extract_aliyun_data(raw)
        blocks = ocr.normalize_aliyun_blocks(data)
        text = ocr.extract_aliyun_text(data)

        self.assertEqual(text, "Total Assets")
        self.assertEqual(blocks[0]["text"], "Total")
        self.assertEqual(blocks[0]["bounding_box"]["width"], 3)

    def test_normalize_aliyun_unified_blocks(self):
        raw = {
            "Data": {
                "Content": "Balance Sheet",
                "SubImages": [
                    {
                        "Type": "Text",
                        "SubImageId": 1,
                        "BlockInfo": {
                            "BlockDetails": [
                                {
                                    "BlockContent": "Balance Sheet",
                                    "BlockConfidence": 97,
                                    "BlockRect": {"CenterX": 100, "CenterY": 50, "Width": 120, "Height": 20},
                                    "BlockPoints": [{"X": 40, "Y": 40}, {"X": 160, "Y": 40}],
                                    "CharInfos": [{"CharContent": "B"}],
                                }
                            ]
                        },
                    }
                ],
            }
        }

        data = ocr.extract_aliyun_data(raw)
        blocks = ocr.normalize_aliyun_blocks(data)
        text = ocr.extract_aliyun_text(data)

        self.assertEqual(text, "Balance Sheet")
        self.assertEqual(blocks[0]["text"], "Balance Sheet")
        self.assertEqual(blocks[0]["bounding_box"]["Width"], 120)
        self.assertEqual(blocks[0]["sub_image_type"], "Text")

    def test_normalize_aliyun_table_blocks(self):
        page_data = {
            "content": "表格内容",
            "prism_tablesInfo": [
                {
                    "tableId": 0,
                    "cellInfos": [
                        {
                            "tableCellId": 10,
                            "word": "现金",
                            "xsc": 0,
                            "xec": 1,
                            "ysc": 2,
                            "yec": 2,
                            "pos": [{"x": 1, "y": 2}],
                        }
                    ],
                }
            ],
        }

        blocks = ocr.normalize_aliyun_table_blocks(page_data)

        self.assertEqual(blocks[0]["text"], "现金")
        self.assertEqual(blocks[0]["table_id"], 0)
        self.assertEqual(blocks[0]["cell_range"]["xsc"], 0)

    def test_resolve_requested_methods(self):
        self.assertEqual(ocr.resolve_requested_methods("tencent_table_v3", None), ["tencent_table_v3"])
        self.assertEqual(ocr.resolve_requested_methods(None, "both"), ["tencent_text", "aliyun_text"])

    def test_process_pdf_with_provider_writes_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "output"
            pdf_path = Path(tmpdir) / "audit.pdf"
            pdf_path.write_bytes(b"%PDF-1.4")

            provider = FakeProvider(
                "tencent_table_v3",
                {
                    1: {
                        "raw": {"request_id": "r-1"},
                        "text": "table text",
                        "blocks": [{"text": "table text"}],
                        "artifacts": [{"filename": "page_0001.xlsx", "bytes": b"excel-bytes"}],
                    }
                },
            )

            failures = ocr.process_pdf_with_provider(
                pdf_path=pdf_path,
                rendered_pages=[ocr.RenderedPage(page_number=1, image_bytes=b"img", width=100, height=200)],
                provider=provider,
                output_root=output_dir,
            )

            self.assertEqual(failures, [])
            artifact_path = output_dir / "tencent_table_v3" / "audit" / "artifacts" / "page_0001.xlsx"
            self.assertTrue(artifact_path.exists())
            self.assertEqual(artifact_path.read_bytes(), b"excel-bytes")

    def test_should_abort_provider_after_error_for_resource_package(self):
        self.assertTrue(
            ocr.should_abort_provider_after_error(
                "[TencentCloudSDKException] code:ResourceUnavailable.ResourcePackageRunOut message:账号资源包耗尽。"
            )
        )

    def test_main_returns_one_for_empty_input_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            secret_path = Path(tmpdir) / "secret"
            input_dir.mkdir()
            output_dir.mkdir()
            self.write_secret(secret_path)

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = ocr.main(
                    [
                        "--provider",
                        "tencent",
                        "--input",
                        str(input_dir),
                        "--output",
                        str(output_dir),
                        "--secret",
                        str(secret_path),
                    ]
                )

            self.assertEqual(code, 1)
            self.assertIn("No PDF files found", stdout.getvalue())

    def test_main_returns_one_for_bad_credentials(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            secret_path = Path(tmpdir) / "secret"
            input_dir.mkdir()
            output_dir.mkdir()
            (input_dir / "audit.pdf").write_bytes(b"%PDF-1.4")
            secret_path.write_text("Tencent:\nSecretId:\n", encoding="utf-8")

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = ocr.main(
                    [
                        "--provider",
                        "tencent",
                        "--input",
                        str(input_dir),
                        "--output",
                        str(output_dir),
                        "--secret",
                        str(secret_path),
                    ]
                )

            self.assertEqual(code, 1)
            self.assertIn("Missing value", stdout.getvalue())

    def test_main_writes_outputs_for_successful_provider_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            secret_path = Path(tmpdir) / "secret"
            input_dir.mkdir()
            output_dir.mkdir()
            pdf_path = input_dir / "audit.pdf"
            pdf_path.write_bytes(b"%PDF-1.4")
            self.write_secret(secret_path)

            provider = FakeProvider(
                "tencent",
                {
                    1: {
                        "raw": {"request_id": "r-1"},
                        "text": "hello world",
                        "blocks": [{"text": "hello world"}],
                    }
                },
            )

            with mock.patch.object(ocr, "build_provider_clients", return_value=[provider]), mock.patch.object(
                ocr,
                "render_pdf_pages",
                return_value=[ocr.RenderedPage(page_number=1, image_bytes=b"img", width=100, height=200)],
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    code = ocr.main(
                        [
                            "--provider",
                            "tencent",
                            "--input",
                            str(input_dir),
                            "--output",
                            str(output_dir),
                            "--secret",
                            str(secret_path),
                        ]
                    )

            self.assertEqual(code, 0)
            result_json = output_dir / "tencent" / "audit" / "result.json"
            result_txt = output_dir / "tencent" / "audit" / "result.txt"
            raw_json = output_dir / "tencent" / "audit" / "raw" / "page_0001.json"
            self.assertTrue(result_json.exists())
            self.assertTrue(result_txt.exists())
            self.assertTrue(raw_json.exists())
            parsed = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(parsed["provider"], "tencent")
            self.assertEqual(parsed["pages"][0]["text"], "hello world")

    def test_main_returns_one_and_sanitizes_failed_page_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            secret_path = Path(tmpdir) / "secret"
            input_dir.mkdir()
            output_dir.mkdir()
            pdf_path = input_dir / "audit.pdf"
            pdf_path.write_bytes(b"%PDF-1.4")
            self.write_secret(secret_path)

            provider = FakeProvider(
                "aliyun",
                {
                    1: {
                        "raw": {"request_id": "r-1"},
                        "text": "page one",
                        "blocks": [{"text": "page one"}],
                    },
                    2: RuntimeError("provider exploded: top-secret-value"),
                },
                secrets=["top-secret-value"],
            )

            with mock.patch.object(ocr, "build_provider_clients", return_value=[provider]), mock.patch.object(
                ocr,
                "render_pdf_pages",
                return_value=[
                    ocr.RenderedPage(page_number=1, image_bytes=b"img1", width=100, height=200),
                    ocr.RenderedPage(page_number=2, image_bytes=b"img2", width=100, height=200),
                ],
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    code = ocr.main(
                        [
                            "--provider",
                            "aliyun",
                            "--input",
                            str(input_dir),
                            "--output",
                            str(output_dir),
                            "--secret",
                            str(secret_path),
                        ]
                    )

            self.assertEqual(code, 1)
            self.assertNotIn("top-secret-value", stdout.getvalue())
            result_json = output_dir / "aliyun" / "audit" / "result.json"
            parsed = json.loads(result_json.read_text(encoding="utf-8"))
            self.assertEqual(parsed["pages"][1]["error"], "provider exploded: [REDACTED]")

    def test_process_pdf_with_provider_short_circuits_after_fatal_error(self):
        class FatalProvider(ocr.OCRProvider):
            name = "tencent"

            def __init__(self):
                self.calls = 0

            def recognize_page(self, page):
                self.calls += 1
                raise RuntimeError("FailedOperation.UnOpenError: service not open")

        with tempfile.TemporaryDirectory() as tmpdir:
            provider = FatalProvider()
            output_dir = Path(tmpdir) / "output"
            pdf_path = Path(tmpdir) / "audit.pdf"
            pdf_path.write_bytes(b"%PDF-1.4")
            rendered_pages = [
                ocr.RenderedPage(page_number=1, image_bytes=b"1", width=100, height=100),
                ocr.RenderedPage(page_number=2, image_bytes=b"2", width=100, height=100),
                ocr.RenderedPage(page_number=3, image_bytes=b"3", width=100, height=100),
            ]

            failures = ocr.process_pdf_with_provider(
                pdf_path=pdf_path,
                rendered_pages=rendered_pages,
                provider=provider,
                output_root=output_dir,
            )

            self.assertEqual(provider.calls, 1)
            self.assertEqual(len(failures), 3)
            parsed = json.loads(
                (output_dir / "tencent" / "audit" / "result.json").read_text(encoding="utf-8")
            )
            self.assertIn("Skipped after fatal provider error", parsed["pages"][1]["error"])

    def test_main_runs_both_providers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_dir = Path(tmpdir) / "input"
            output_dir = Path(tmpdir) / "output"
            secret_path = Path(tmpdir) / "secret"
            input_dir.mkdir()
            output_dir.mkdir()
            pdf_path = input_dir / "audit.pdf"
            pdf_path.write_bytes(b"%PDF-1.4")
            self.write_secret(secret_path)

            tencent_provider = FakeProvider(
                "tencent",
                {1: {"raw": {"id": "t1"}, "text": "tencent page", "blocks": []}},
            )
            aliyun_provider = FakeProvider(
                "aliyun",
                {1: {"raw": {"id": "a1"}, "text": "aliyun page", "blocks": []}},
            )

            with mock.patch.object(
                ocr,
                "build_provider_clients",
                return_value=[tencent_provider, aliyun_provider],
            ), mock.patch.object(
                ocr,
                "render_pdf_pages",
                return_value=[ocr.RenderedPage(page_number=1, image_bytes=b"img", width=100, height=200)],
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    code = ocr.main(
                        [
                            "--provider",
                            "both",
                            "--input",
                            str(input_dir),
                            "--output",
                            str(output_dir),
                            "--secret",
                            str(secret_path),
                        ]
                    )

            self.assertEqual(code, 0)
            self.assertTrue((output_dir / "tencent" / "audit" / "result.json").exists())
            self.assertTrue((output_dir / "aliyun" / "audit" / "result.json").exists())


if __name__ == "__main__":
    unittest.main()
