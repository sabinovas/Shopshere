import re

from conftest import load_module

pdfs = load_module("data_generator/make_policy_pdfs.py", "make_policy_pdfs")


def test_every_document_is_a_structurally_valid_pdf(tmp_path):
    for name, (title, sections) in pdfs.DOCS.items():
        path = tmp_path / name
        pages = pdfs.build_pdf(title, sections, path)
        data = path.read_bytes()
        assert pages >= 1
        assert data.startswith(b"%PDF-1.4")
        assert data.rstrip().endswith(b"%%EOF")

        # The xref table must point at each "n 0 obj" header
        xref_pos = int(re.search(rb"startxref\n(\d+)", data).group(1))
        assert data[xref_pos:xref_pos + 4] == b"xref"
        count = int(re.search(rb"xref\n0 (\d+)", data).group(1))
        offsets = [int(m) for m in re.findall(rb"(\d{10}) 00000 n ", data)]
        assert len(offsets) == count - 1
        for n, off in enumerate(offsets, start=1):
            assert data[off:].startswith(f"{n} 0 obj".encode())


def test_every_page_says_the_document_is_synthetic(tmp_path):
    title, sections = pdfs.DOCS["shopsphere_returns_refunds.pdf"]
    path = tmp_path / "doc.pdf"
    pages = pdfs.build_pdf(title, sections, path)
    assert path.read_bytes().count(b"Not a real policy") >= pages


def test_escape_handles_pdf_special_characters():
    assert pdfs.pdf_escape("a (b) c\\d") == "a \\(b\\) c\\\\d"
