"""Writes synthetic ShopSphere policy PDFs for the Week 7 RAG demo (no external packages).

The documents are invented for this portfolio project and say so on every page. They give
the assistant something to retrieve that the Olist tables don't have: shipping, returns,
payment and seller rules. Facts are kept consistent with the Olist data (Brazil, BRL,
boleto and credit-card instalments, estimated delivery dates).

    python data_generator/make_policy_pdfs.py <output folder>
"""

import sys
import textwrap
from pathlib import Path

DISCLAIMER = "Synthetic document created for the ShopSphere portfolio project. Not a real policy."

DOCS = {
    "shopsphere_shipping_policy.pdf": ("ShopSphere Shipping and Delivery Policy", [
        ("1. Delivery estimates", "Every order shows an estimated delivery date at checkout. The estimate is calculated from the seller's handling time, the carrier's transit time and the distance between the seller's state and the customer's zip code. Orders inside the same state usually arrive within 5 to 10 business days. Orders to the North and Northeast regions can take 15 to 25 business days."),
        ("2. Handling time", "Sellers must hand the parcel to the carrier before the shipping limit date shown on the order. If a seller misses the shipping limit date twice in 30 days, their listings are paused for review."),
        ("3. Late deliveries", "An order is late when it is delivered after the estimated delivery date. If an order is more than 7 days late, the customer may cancel it for a full refund, including freight. If an order is late but the customer keeps it, ShopSphere refunds the freight value as store credit within 5 business days."),
        ("4. Tracking", "Tracking is available once the order status is 'shipped'. Status values are: created, approved, invoiced, processing, shipped, delivered, canceled and unavailable. 'Unavailable' means the seller could not fulfil the order; the customer is refunded automatically."),
        ("5. Freight", "Freight is charged per item and shown separately from the product price. Freight above 30% of the item price is flagged to the seller for review."),
    ]),
    "shopsphere_returns_refunds.pdf": ("ShopSphere Returns and Refunds Policy", [
        ("1. Right of withdrawal", "Under the Brazilian Consumer Defense Code (Article 49), customers may cancel an online purchase within 7 days of receiving it, for any reason. The product must be returned in its original condition. Return freight is paid by ShopSphere."),
        ("2. Defective or wrong items", "If an item arrives damaged, defective, different from the listing, or incomplete, the customer can request a replacement or a full refund within 30 days of delivery for non-durable goods and 90 days for durable goods. Photos of the problem speed up the process."),
        ("3. Missing items", "If an order with several items arrives incomplete, the customer should open a ticket within 7 days. The missing items are reshipped or refunded. Orders from more than one seller can arrive in separate parcels on different dates."),
        ("4. Refund method and timing", "Refunds go back to the original payment method. Credit card refunds appear on the next one or two statements. Boleto payments are refunded by bank transfer within 10 business days. Vouchers are reissued as new vouchers."),
        ("5. Instalments", "When a purchase paid in instalments is refunded, the remaining instalments are cancelled and paid instalments are refunded."),
    ]),
    "shopsphere_payments_faq.pdf": ("ShopSphere Payments FAQ", [
        ("Which payment methods are accepted?", "Credit card, debit card, boleto bancario and ShopSphere vouchers. Credit card is the most used method, followed by boleto."),
        ("Can I pay in instalments?", "Yes. Credit card purchases can be split into up to 24 monthly instalments. Instalments above 10 may carry interest set by the card issuer. Debit card, boleto and vouchers are paid in a single payment."),
        ("When is my order approved?", "Credit and debit card payments are usually approved within minutes. Boleto payments are approved when the bank confirms payment, which takes 1 to 3 business days. The order is only sent to the seller after approval."),
        ("Can I combine payment methods?", "Yes. An order can be paid with a voucher plus one other method. Each payment appears as a separate payment line on the order."),
        ("What if my boleto expires?", "An unpaid boleto expires after 3 business days and the order is cancelled automatically. Place the order again to get a new boleto."),
    ]),
    "shopsphere_seller_guidelines.pdf": ("ShopSphere Seller Guidelines", [
        ("1. Listings", "Each product must have a category, at least one photo, the weight in grams and the package dimensions in centimetres. Listings without weight or dimensions cannot calculate freight and are hidden."),
        ("2. Service levels", "Sellers are measured on on-time handover, late-delivery rate, cancellation rate and average review score. A late-delivery rate above 10% or an average review score below 3.5 over 90 days triggers a performance review."),
        ("3. Customer reviews", "Customers can review an order from the delivery date. Sellers may reply publicly once. Asking customers to change a review in exchange for a refund or discount is not allowed."),
        ("4. Payouts", "Seller payouts are made 30 days after delivery, minus the marketplace commission and any refunds issued for the seller's orders."),
    ]),
}


def pdf_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)").encode("latin-1", "replace").decode("latin-1")


def build_pdf(title: str, sections, path: Path) -> int:
    """Lay out text lines on A4 pages with Helvetica and write a minimal valid PDF."""
    lines = [("title", title), ("small", DISCLAIMER), ("gap", "")]
    for heading, body in sections:
        lines.append(("heading", heading))
        lines += [("body", line) for line in textwrap.wrap(body, 92)]
        lines.append(("gap", ""))

    style = {"title": (18, 26), "heading": (12, 18), "body": (10, 14), "small": (8, 12), "gap": (10, 8)}
    pages, current, y = [], [], 800
    for kind, text in lines:
        size, lead = style[kind]
        if y - lead < 60:
            pages.append(current)
            current, y = [], 800
        y -= lead
        if text:
            font = "F2" if kind in ("title", "heading") else "F1"
            current.append(f"BT /{font} {size} Tf 50 {y} Td ({pdf_escape(text)}) Tj ET")
    pages.append(current)

    objects = []  # (number, bytes)

    def add(body: str) -> int:
        objects.append(body.encode("latin-1"))
        return len(objects)

    font1 = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")
    font2 = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>")
    pages_num = len(objects) + 1 + 2 * len(pages) + 1  # placeholder, fixed below
    page_nums = []
    content_nums = []
    for i, ops in enumerate(pages):
        footer = f"BT /F1 8 Tf 50 30 Td ({pdf_escape(DISCLAIMER)}   Page {i + 1} of {len(pages)}) Tj ET"
        stream = "\n".join(ops + [footer])
        content_nums.append(add(f"<< /Length {len(stream.encode('latin-1'))} >>\nstream\n{stream}\nendstream"))
        page_nums.append(add("PAGE_PLACEHOLDER"))
    pages_num = add("PAGES_PLACEHOLDER")
    catalog = add(f"<< /Type /Catalog /Pages {pages_num} 0 R >>")
    info = add(f"<< /Title ({pdf_escape(title)}) /Producer (ShopSphere make_policy_pdfs.py) >>")

    for pn, cn in zip(page_nums, content_nums):
        objects[pn - 1] = (
            f"<< /Type /Page /Parent {pages_num} 0 R /MediaBox [0 0 595 842] "
            f"/Resources << /Font << /F1 {font1} 0 R /F2 {font2} 0 R >> >> /Contents {cn} 0 R >>"
        ).encode("latin-1")
    objects[pages_num - 1] = (
        f"<< /Type /Pages /Kids [{' '.join(f'{p} 0 R' for p in page_nums)}] /Count {len(page_nums)} >>"
    ).encode("latin-1")

    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = []
    for n, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{n} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    out += "".join(f"{o:010d} 00000 n \n" for o in offsets).encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root {catalog} 0 R /Info {info} 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    path.write_bytes(out)
    return len(pages)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python make_policy_pdfs.py <output folder>")
    folder = Path(sys.argv[1])
    folder.mkdir(parents=True, exist_ok=True)
    for name, (title, sections) in DOCS.items():
        n = build_pdf(title, sections, folder / name)
        print(f"wrote {folder / name} ({n} page{'s' if n > 1 else ''})")
