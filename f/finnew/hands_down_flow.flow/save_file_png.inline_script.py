import csv
import json
import io
import boto3
from PIL import Image, ImageDraw, ImageFont
import sys  # Import sys to print to stderr for logs
import gdown

# Google Drive shareable link (yours)
drive_url = "https://drive.google.com/uc?id=1Ot09tMECwU9OgWY2dgOX9julzhG6Xn22"
local_font_path = "CustomFont.ttf"

# Download font file
gdown.download(drive_url, local_font_path, quiet=False)


# --- Configuration ---
# Define colors (approximate from the reference image)
COLOR_YELLOW = (255, 255, 0)
COLOR_ORANGE = (255, 165, 0)
COLOR_LIGHT_GREY = (220, 220, 220)  # For alternating rows
COLOR_DARK_GREY = (169, 169, 169)  # For section headers like "Khối ngoại"
COLOR_WHITE = (255, 255, 255)
COLOR_BLACK = (0, 0, 0)
COLOR_GREEN = (0, 128, 0)
COLOR_RED = (255, 0, 0)

# Define font and sizes
# Using a try-except block for robustness in different environments
try:
    # Try loading a common font available on most systems
    FONT_TITLE = ImageFont.truetype("CustomFont.ttf", 20)  # Bold Arial for title
    FONT_HEADER = ImageFont.truetype(
        "CustomFont.ttf", 14
    )  # Bold Arial for table headers
    FONT_BODY = ImageFont.truetype("CustomFont.ttf", 14)  # Regular Arial for body text
    FONT_BOLD_BODY = ImageFont.truetype(
        "CustomFont.ttf", 14
    )  # Bold Arial for section headers
    print("Using CustomFont fonts.", file=sys.stderr)
except IOError:
    # Fallback to a default font if arial.ttf is not found - safest for portability
    print(
        "Warning: CustomFont font not found. Using default PIL font. Output quality may vary.",
        file=sys.stderr,
    )
    FONT_TITLE = ImageFont.load_default()
    FONT_HEADER = ImageFont.load_default()
    FONT_BODY = ImageFont.load_default()
    FONT_BOLD_BODY = ImageFont.load_default()


# Padding and spacing
PADDING = 10
ROW_HEIGHT = 25  # Fixed height for table rows and header bars
SECTION_SPACING = 15
TABLE_ROW_SPACING = 5
IMAGE_WIDTH = 800  # Fixed width for the image

COL_WIDTHS = [100, 100, 80, 80, 120, 100, 150]  # Base widths, adjust as needed
# Spacing AFTER each column (6 values for the 7 columns)
# Index 4 is KLGD, Index 5 is GTGD. Spacing AFTER KLGD is at index 4.
INTER_COL_SPACINGS = [PADDING, PADDING, PADDING, PADDING, PADDING * 1.5, PADDING]


# --- Data Parsing ---
def parse_csv_data(csv_string: str) -> dict:
    """Parses the CSV string content into a structured dictionary."""
    data = {
        "title": "",
        "indices": [],
        "foreign_investor": {},
        "proprietary_trading": {},
        "highlighted_sectors": [],
        "focus_stocks": [],
        "impact_increase": {},
        "impact_decrease": {},
    }

    # Use io.StringIO to treat the string as a file
    csvfile = io.StringIO(csv_string)
    # csv.reader handles different line endings and quoting
    reader = csv.reader(csvfile)

    current_section = None

    # Read header row separately to identify columns if needed, but simple iteration works
    # Assuming the structure is relatively fixed based on the example

    for row in reader:
        if not row:  # Skip empty rows or rows starting with BOM
            continue

        first_cell = row[0].strip()
        print(first_cell)
        if "TỔNG KẾT GIAO DỊCH PHIÊN" in first_cell:
            data["title"] = first_cell
            # The next meaningful line should be the table header, handled below
            current_section = "table_header"
        elif "CHỈ SỐ" in first_cell and current_section == "table_header":
            # This is the table header row, ignore it and set next state
            current_section = "indices"
        elif current_section == "indices" and len(row) >= 7:
            # Check if the first cell looks like an index name
            if first_cell in [
                "VNINDEX",
                "VN30",
                "HNXIndex",
                "HNX30",
                "UPCOM",
                "VNXALL",
            ]:
                data["indices"].append(row)
            else:
                # Assuming indices section ends when the first cell is not an index name
                # This row might belong to the next section, re-process it below
                current_section = None  # Stop parsing as indices

        # Process sections after the table
        # Use elif to process the current row if it wasn't an index row
        if first_cell.startswith("Khối ngoại:"):
            if len(row) >= 3:
                data["foreign_investor"] = {
                    "header": first_cell,
                    "volume_change": row[1].strip() if len(row) > 1 else "",
                    "value_change": row[2].strip() if len(row) > 2 else "",
                }
            current_section = "foreign_details"
        elif first_cell == "Top mua ròng:":
            if len(row) > 1:
                # The stocks are in the second cell, potentially quoted
                data["foreign_investor"]["top_buy"] = [
                    stock.strip()
                    for stock in row[1].strip('"').split(",")
                    if stock.strip()
                ]
            # current_section remains 'foreign_details'
        elif first_cell == "Top bán ròng:":
            if len(row) > 1:
                data["foreign_investor"]["top_sell"] = [
                    stock.strip()
                    for stock in row[1].strip('"').split(",")
                    if stock.strip()
                ]
            # current_section remains 'foreign_details'
        elif first_cell.startswith("Khối tự doanh:"):
            if len(row) >= 2:
                data["proprietary_trading"] = {
                    "header": first_cell,
                    "value_change": row[1].strip() if len(row) > 1 else "",
                }
            current_section = "proprietary_details"
        elif first_cell == "Nhóm ngành nổi bật:":
            if len(row) > 1:
                data["highlighted_sectors"] = [
                    sector.strip()
                    for sector in row[1].strip('"').split(",")
                    if sector.strip()
                ]
            current_section = "highlighted_sectors"
        elif first_cell == "Cổ phiếu tâm điểm:":
            if len(row) > 1:
                data["focus_stocks"] = [
                    stock.strip()
                    for stock in row[1].strip('"').split(",")
                    if stock.strip()
                ]
            current_section = "focus_stocks"
        elif first_cell.startswith("Tác động tăng"):  # Handles 'Tác động tăng (+X.XX):'
            if len(row) > 1:
                value_part = (
                    first_cell.split("(")[-1].split(")")[0] if "(" in first_cell else ""
                )
                stocks_str = row[1].strip('"')
                data["impact_increase"] = {
                    "value": value_part,
                    "stocks": [
                        stock.strip()
                        for stock in stocks_str.split(",")
                        if stock.strip()
                    ],
                }
            current_section = "impact_increase"
        elif first_cell.startswith("Tác động giảm"):  # Handles 'Tác động giảm (-X.XX):'
            if len(row) > 1:
                value_part = (
                    first_cell.split("(")[-1].split(")")[0] if "(" in first_cell else ""
                )
                stocks_str = row[1].strip('"')
                data["impact_decrease"] = {
                    "value": value_part,
                    "stocks": [
                        stock.strip()
                        for stock in stocks_str.split(",")
                        if stock.strip()
                    ],
                }
            current_section = "impact_decrease"

    return data


# --- Drawing ---
def draw_stock_report(data: dict) -> Image.Image:
    """Draws the stock report based on parsed data onto a PIL Image with grid lines."""
    current_height = PADDING

    # Create a base image - estimate height generously, crop later
    img = Image.new(
        "RGB", (IMAGE_WIDTH, 2000), color=COLOR_WHITE
    )  # Increased initial height estimate
    d = ImageDraw.Draw(img)

    # Helper function to draw text vertically centered in a given row height
    def draw_text_centered_v(draw, x, y_top_of_row, row_height, text, font, fill_color):
        text_bbox = draw.textbbox((0, 0), text, font=font)
        text_height = text_bbox[3] - text_bbox[1]
        y_centered = y_top_of_row + (row_height - text_height) // 2
        draw.text((x, y_centered), text, fill=fill_color, font=font)

    # Helper function to draw a list line (Header: Item1, Item2, ...)
    def draw_list_line(
        draw,
        y_start,
        header_text,
        items_list,
        header_font,
        items_font,
        header_color,
        items_color,
    ):
        """Draws a line with a header and a comma-separated list of items, returns line height including spacing."""
        header_x = PADDING
        draw.text((header_x, y_start), header_text, fill=header_color, font=header_font)

        items_text = ",".join(items_list)
        items_x = PADDING + draw.textlength(
            header_text, font=header_font
        )  # Start drawing items after header

        draw.text((items_x, y_start), items_text, fill=items_color, font=items_font)

        # Calculate line height needed based on maximum text height on this line
        header_height = (
            draw.textbbox((0, 0), header_text, font=header_font)[3]
            - draw.textbbox((0, 0), header_text, font=header_font)[1]
        )
        # Calculate height of items text - assumes it fits on one line for simplicity
        items_height = (
            draw.textbbox((0, 0), items_text if items_text else "A", font=items_font)[3]
            - draw.textbbox((0, 0), items_text if items_text else "A", font=items_font)[
                1
            ]
        )  # Use "A" if list is empty to get font height

        line_content_height = max(header_height, items_height)
        return line_content_height + TABLE_ROW_SPACING  # Return height + spacing

    # 1. Draw Title
    title_text = data.get("title", "TỔNG KẾT GIAO DỊCH")
    title_text_bbox = d.textbbox((0, 0), title_text, font=FONT_TITLE)
    title_text_height = title_text_bbox[3] - title_text_bbox[1]
    title_bg_height = title_text_height + PADDING * 2  # Bar height
    title_y = current_height + PADDING  # Text Y position, centered within the bar
    title_x = (IMAGE_WIDTH - d.textlength(title_text, font=FONT_TITLE)) // 2

    d.rectangle(
        [(0, current_height), (IMAGE_WIDTH, current_height + title_bg_height)],
        fill=COLOR_YELLOW,
    )
    d.text((title_x, title_y), title_text, fill=COLOR_BLACK, font=FONT_TITLE)
    current_height += title_bg_height

    current_height += SECTION_SPACING  # Space after title

    # 2. Draw Table (Revised with Lines and Spacing)
    table_data = data.get("indices", [])
    if table_data:
        headers = [
            "CHỈ SỐ",
            "ĐIỂM",
            "(+/-)",
            "(+/-)%",
            "KLGD (triệu cp)",
            "GTGD (tỷ)",
            "CP tăng/giảm",
        ]

        # Calculate column x positions based on widths and spacings
        # col_x[i] is the x coordinate where the text for column i starts
        col_x = [PADDING]
        for i in range(len(COL_WIDTHS) - 1):
            col_x.append(col_x[-1] + COL_WIDTHS[i] + INTER_COL_SPACINGS[i])

        table_start_y = current_height  # Top of the orange header bar

        # Draw header row
        header_y = current_height
        d.rectangle(
            [(0, header_y), (IMAGE_WIDTH, header_y + ROW_HEIGHT)], fill=COLOR_ORANGE
        )

        for i, header in enumerate(headers):
            if i < len(col_x):
                draw_text_centered_v(
                    d, col_x[i], header_y, ROW_HEIGHT, header, FONT_HEADER, COLOR_WHITE
                )

        # Draw header bottom horizontal line
        d.line(
            [
                (0, header_y),
                (IMAGE_WIDTH, header_y),
            ],
            fill=COLOR_BLACK,
            width=1,
        )
        current_height += ROW_HEIGHT  # Advance past the header row

        # Draw data rows
        for i, row in enumerate(table_data):
            row_y = current_height  # Top of the current data row
            bg_color = COLOR_LIGHT_GREY if i % 2 == 0 else COLOR_WHITE
            d.rectangle([(0, row_y), (IMAGE_WIDTH, row_y + ROW_HEIGHT)], fill=bg_color)

            for j, cell_data in enumerate(row):
                if j < len(col_x):
                    text_to_draw = str(cell_data).strip()

                    if j == 6:  # 'CP tăng/giảm' column - custom drawing for splits
                        parts = text_to_draw.split("|")
                        part_x = col_x[j]
                        # Calculate vertical center offset once for this row type
                        body_font_height = (
                            d.textbbox((0, 0), "A", font=FONT_BODY)[3]
                            - d.textbbox((0, 0), "A", font=FONT_BODY)[1]
                        )
                        body_text_y_offset = (ROW_HEIGHT - body_font_height) // 2

                        for k, part in enumerate(parts):
                            part_text = part.strip()
                            color = (
                                COLOR_GREEN
                                if k == 0
                                else COLOR_BLACK
                                if k == 1
                                else COLOR_RED
                            )
                            d.text(
                                (part_x, row_y + body_text_y_offset),
                                part_text + ("|" if k < len(parts) - 1 else ""),
                                fill=color,
                                font=FONT_BODY,
                            )
                            # Move position for the next part (+ space for '|')
                            part_x += (
                                d.textlength(part_text, font=FONT_BODY)
                                + (
                                    d.textlength("|", font=FONT_BODY)
                                    if k < len(parts) - 1
                                    else 0
                                )
                                + 2
                            )  # Small space after text

                    else:  # Other columns - use centered helper
                        text_color = COLOR_BLACK  # Default color
                        if j in [2, 3]:  # Apply color for (+/-) and (+/-)%
                            if text_to_draw.startswith("↑") or text_to_draw.startswith(
                                "+"
                            ):
                                text_color = COLOR_GREEN
                            elif text_to_draw.startswith(
                                "↓"
                            ) or text_to_draw.startswith("-"):
                                text_color = COLOR_RED

                        draw_text_centered_v(
                            d,
                            col_x[j],
                            row_y,
                            ROW_HEIGHT,
                            text_to_draw,
                            FONT_BODY,
                            text_color,
                        )

            # Draw bottom horizontal line for this data row
            d.line(
                [(0, row_y), (IMAGE_WIDTH, row_y)],
                fill=COLOR_BLACK,
                width=1,
            )
            current_height += ROW_HEIGHT  # Advance past the data row

        table_end_y = current_height  # Bottom edge is after the last row's bottom line

        # Draw vertical lines covering the whole table height (from header top to last data row bottom)
        # Left border line (at the start of the first column's text area = PADDING)
        d.line(
            [(0, table_end_y), (IMAGE_WIDTH, table_end_y)],
            fill=COLOR_BLACK,
            width=1,
        )

        # Vertical lines between columns
        # There are len(COL_WIDTHS) - 1 = 6 lines
        for i in range(len(COL_WIDTHS) - 1):
            # Line position is after column i's width and half of the spacing that follows it
            # X position is col_x[i] + COL_WIDTHS[i] + INTER_COL_SPACINGS[i]/2
            if i == 0:
                line_x = 0 + COL_WIDTHS[i] + INTER_COL_SPACINGS[i] / 2
            else:
                line_x = col_x[i] + COL_WIDTHS[i] + INTER_COL_SPACINGS[i] / 2
            d.line(
                [(line_x, table_start_y), (line_x, table_end_y)],
                fill=COLOR_BLACK,
                width=1,
            )

        # Right border line (after the last column's width and padding)
        # The end of the last column's text area is col_x[-1] + COL_WIDTHS[-1]
        # The right border is at this position + PADDING (the padding after the last column)
        right_border_x = col_x[-1] + COL_WIDTHS[-1] + PADDING
        d.line(
            [(right_border_x, table_start_y), (right_border_x, table_end_y)],
            fill=COLOR_BLACK,
            width=1,
        )

    current_height += 1  # Space after table

    # 3. Draw Khối ngoại (Revised - Left Aligned Stats)
    foreign_data = data.get("foreign_investor", {})
    if foreign_data:
        header_y = current_height
        d.rectangle(
            [(0, header_y), (IMAGE_WIDTH, header_y + ROW_HEIGHT)], fill=COLOR_WHITE
        )

        # Draw header text and stats text within the dark grey bar, vertically centered
        # Use a reference text like "A" to get approximate font height for centering
        ref_text_bbox = d.textbbox((0, 0), "A", font=FONT_BOLD_BODY)
        text_y_offset = (
            ROW_HEIGHT - (ref_text_bbox[3] - ref_text_bbox[1])
        ) // 2  # Vertical center offset within the bar

        header_text = foreign_data.get("header", "")
        volume_text = foreign_data.get("volume_change", "")
        value_text = foreign_data.get("value_change", "")

        # Determine color based on '+' or '↑' for green, otherwise red
        volume_color = (
            COLOR_GREEN if "+" in volume_text or "↑" in volume_text else COLOR_RED
        )
        value_color = (
            COLOR_GREEN if "+" in value_text or "↑" in value_text else COLOR_RED
        )

        # Draw Header "Khối ngoại:":
        header_x = PADDING
        # Header text is RED in the new reference image
        d.text(
            (header_x, header_y + text_y_offset),
            header_text,
            fill=COLOR_BLACK,
            font=FONT_BOLD_BODY,
        )

        # Draw Volume (start after header text + space):
        volume_x = PADDING + d.textlength(header_text, font=FONT_BOLD_BODY) + PADDING
        d.text(
            (volume_x, header_y + text_y_offset),
            volume_text,
            fill=volume_color,
            font=FONT_BODY,
        )

        # Draw Value (start after volume text + space):
        value_x = volume_x + d.textlength(volume_text, font=FONT_BODY) + PADDING
        d.text(
            (value_x, header_y + text_y_offset),
            value_text,
            fill=value_color,
            font=FONT_BODY,
        )

        current_height += ROW_HEIGHT  # Advance past the header bar
        # current_height += TABLE_ROW_SPACING  # Add spacing after the bar
        d.line(
            [(0, current_height), (IMAGE_WIDTH, current_height)],
            fill=COLOR_BLACK,
            width=1,
        )
        current_height += TABLE_ROW_SPACING
        # Draw Top Buy list (using existing helper)
        top_buy_list = foreign_data.get("top_buy", [])
        if top_buy_list:
            line_height = draw_list_line(
                d,
                current_height,
                "Top mua ròng: ",
                top_buy_list,
                FONT_BOLD_BODY,
                FONT_BODY,
                COLOR_BLACK,
                COLOR_GREEN,
            )
            current_height += line_height

        d.line(
            [(0, current_height), (IMAGE_WIDTH, current_height)],
            fill=COLOR_BLACK,
            width=1,
        )

        current_height += TABLE_ROW_SPACING  # Add spacing after the bar

        # Draw Top Sell list (using existing helper)
        top_sell_list = foreign_data.get("top_sell", [])
        if top_sell_list:
            line_height = draw_list_line(
                d,
                current_height,
                "Top bán ròng: ",
                top_sell_list,
                FONT_BOLD_BODY,
                FONT_BODY,
                COLOR_BLACK,
                COLOR_RED,
            )
            current_height += line_height

        d.line(
            [(0, current_height), (IMAGE_WIDTH, current_height)],
            fill=COLOR_BLACK,
            width=1,
        )
        # current_height += TABLE_ROW_SPACING

    # current_height += ROW_HEIGHT  # Space after foreign investor section
    current_height += 1
    # 4. Draw Khối tự doanh (Revised - Left Aligned Stats)
    proprietary_data = data.get("proprietary_trading", {})
    if proprietary_data:
        header_y = current_height
        d.rectangle(
            [(0, header_y), (IMAGE_WIDTH, header_y + ROW_HEIGHT)], fill=COLOR_WHITE
        )

        # Draw header text and value text within the dark grey bar, vertically centered
        ref_text_bbox = d.textbbox((0, 0), "A", font=FONT_BOLD_BODY)
        text_y_offset = (
            ROW_HEIGHT - (ref_text_bbox[3] - ref_text_bbox[1])
        ) // 2  # Vertical center offset

        header_text = proprietary_data.get("header", "")
        value_text = proprietary_data.get("value_change", "")
        # Determine color based on '+' or '↑' for green, otherwise red
        value_color = (
            COLOR_GREEN if "+" in value_text or "↑" in value_text else COLOR_RED
        )

        # Draw Header "Khối tự doanh:":
        header_x = PADDING
        # Header text is RED in the new reference image
        d.text(
            (header_x, header_y + text_y_offset),
            header_text,
            fill=COLOR_BLACK,
            font=FONT_BOLD_BODY,
        )

        # Draw Value (start after header text + space):
        value_x = PADDING + d.textlength(header_text, font=FONT_BOLD_BODY) + PADDING
        d.text(
            (value_x, header_y + text_y_offset),
            value_text,
            fill=value_color,
            font=FONT_BODY,
        )

        current_height += ROW_HEIGHT  # Advance past the header bar
        # current_height += TABLE_ROW_SPACING  # Add spacing after the bar

    d.line(
        [(0, current_height), (IMAGE_WIDTH, current_height)],
        fill=COLOR_BLACK,
        width=1,
    )
    current_height += TABLE_ROW_SPACING
    # current_height += ROW_HEIGHT  # Space after proprietary section

    # 5. Draw Nhóm ngành nổi bật (using existing helper)
    sectors_list = data.get("highlighted_sectors", [])
    if sectors_list:
        # draw_list_line calculates height and adds spacing
        line_height = draw_list_line(
            d,
            current_height,
            "Nhóm ngành nổi bật: ",
            sectors_list,
            FONT_BOLD_BODY,
            FONT_BODY,
            COLOR_BLACK,
            COLOR_BLACK,
        )
        current_height += line_height

    # current_height += ROW_HEIGHT  # Space after sectors section
    d.line(
        [(0, current_height), (IMAGE_WIDTH, current_height)],
        fill=COLOR_BLACK,
        width=1,
    )
    current_height += TABLE_ROW_SPACING

    # 6. Draw Cổ phiếu tâm điểm (using existing helper)
    focus_stocks_list = data.get("focus_stocks", [])
    if focus_stocks_list:
        line_height = draw_list_line(
            d,
            current_height,
            "Cổ phiếu tâm điểm: ",
            focus_stocks_list,
            FONT_BOLD_BODY,
            FONT_BODY,
            COLOR_BLACK,
            COLOR_BLACK,
        )
        current_height += line_height

    d.line(
        [(0, current_height), (IMAGE_WIDTH, current_height)],
        fill=COLOR_BLACK,
        width=1,
    )
    current_height += TABLE_ROW_SPACING
    # current_height += ROW_HEIGHT

    # 7. Draw Tác động tăng (using existing helper)
    impact_increase_data = data.get("impact_increase", {})
    stocks_list_increase = impact_increase_data.get("stocks", [])
    if stocks_list_increase:
        header_text = f"Tác động tăng ({impact_increase_data.get('value', '')}): "
        line_height = draw_list_line(
            d,
            current_height,
            header_text,
            stocks_list_increase,
            FONT_BOLD_BODY,
            FONT_BODY,
            COLOR_BLACK,
            COLOR_GREEN,
        )
        current_height += line_height

    d.line(
        [(0, current_height), (IMAGE_WIDTH, current_height)],
        fill=COLOR_BLACK,
        width=1,
    )
    current_height += TABLE_ROW_SPACING

    # 8. Draw Tác động giảm (using existing helper)
    impact_decrease_data = data.get("impact_decrease", {})
    stocks_list_decrease = impact_decrease_data.get("stocks", [])
    if stocks_list_decrease:
        header_text = f"Tác động giảm ({impact_decrease_data.get('value', '')}): "
        line_height = draw_list_line(
            d,
            current_height,
            header_text,
            stocks_list_decrease,
            FONT_BOLD_BODY,
            FONT_BODY,
            COLOR_BLACK,
            COLOR_RED,
        )
        current_height += line_height

    # Adjust image height by cropping any excess white space at the bottom
    final_height = current_height + PADDING
    img = img.crop((0, 0, IMAGE_WIDTH, final_height))

    return img


# --- Main Windmill Function ---
# This function signature is what Windmill expects for inputs
def main(
    s3_bucket_name="ragbucket.hungnq",
    csv_s3_key="stock_summary_report.csv",
    png_s3_key="stock_summary_generated.png",
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="",
    **kwargs,
):
    s3_bucket_name = s3_bucket_name
    csv_s3_key = csv_s3_key
    png_s3_key = png_s3_key
    print(
        f"Starting stock report conversion for s3://{s3_bucket_name}/{csv_s3_key}",
        file=sys.stderr,
    )

    # Windmill environment typically handles AWS credentials automatically
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    # 1. Download CSV from S3
    csv_string = ""
    try:
        print(
            f"Downloading CSV from s3://{s3_bucket_name}/{csv_s3_key}...",
            file=sys.stderr,
        )
        response = s3.get_object(Bucket=s3_bucket_name, Key=csv_s3_key)
        # Read body bytes and decode using utf-8, common for CSV
        csv_string = response["Body"].read().decode("utf-8")
        print("CSV downloaded and decoded.", file=sys.stderr)
    except Exception as e:
        # Return an error dictionary if download fails
        return {"status": "error", "step": "Download CSV", "message": str(e)}

    # 2. Parse CSV Data
    data = {}
    try:
        print("Parsing CSV data...", file=sys.stderr)
        data = parse_csv_data(csv_string)
        print(data)
    except Exception as e:
        # Return an error dictionary if parsing fails
        return {"status": "error", "step": "Parse CSV", "message": str(e)}

    # 3. Draw PNG Image
    image = None
    try:
        print("Drawing PNG image...", file=sys.stderr)
        image = draw_stock_report(data)
        print("PNG image drawn.", file=sys.stderr)
    except Exception as e:
        # Return an error dictionary if drawing fails
        return {"status": "error", "step": "Draw PNG", "message": str(e)}

    # 4. Upload PNG to S3
    try:
        print(
            f"Uploading PNG to s3://{s3_bucket_name}/{png_s3_key}...", file=sys.stderr
        )
        # Use BytesIO to save the image directly to memory
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        image_bytes = buffer.getvalue()  # Get the bytes from the buffer

        s3.put_object(
            Bucket=s3_bucket_name,
            Key=png_s3_key,
            Body=image_bytes,
            ContentType="image/png",  # Specify content type
        )
        print("PNG uploaded successfully.", file=sys.stderr)

    except Exception as e:
        # Return an error dictionary if upload fails
        return {"status": "error", "step": "Upload PNG", "message": str(e)}

    # 5. Return Success Result
    # Windmill will automatically JSON serialize this dictionary
    return {
        "status": "success",
        "message": "CSV successfully converted and PNG uploaded to S3.",
        "uploaded_s3_key": png_s3_key,
        "s3_bucket": s3_bucket_name,
        # You could add a potential S3 URL here if the bucket is public
        # and you know the region (e.g., f"https://{s3_bucket_name}.s3.<REGION>.amazonaws.com/{png_s3_key}")
        # but returning the key and bucket is safer and sufficient for most workflows.
    }


# Note: In a Windmill script, you typically don't need if __name__ == "__main__":
# The `main` function is the entry point executed by the Windmill runtime.
