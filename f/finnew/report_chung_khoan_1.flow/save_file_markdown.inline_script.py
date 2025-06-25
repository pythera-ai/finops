from PIL import Image, ImageDraw, ImageFont
import csv
import re  # For parsing
import boto3  # For S3
import os  # For path operations like basename
from botocore.exceptions import ClientError, NoCredentialsError  # For S3 error handling
import io  # For handling image in memory
from matplotlib import font_manager

# --- S3 Configuration ---
# !!! IMPORTANT: REPLACE with your actual bucket names and keys !!!
S3_BUCKET_NAME = (
    "ragbucket.hungnq"  # Bucket for both CSV and Image, or can be different
)
S3_CSV_KEY = "stock_summary_report.csv"  # Key (path in S3) for the input CSV
S3_IMAGE_KEY = "stock_summary_generated.png"  # Key for the output image
# Optional: Specify the AWS region if your bucket is not in your default configured region
S3_REGION = None  # e.g., "us-east-1", "ap-southeast-1". If None, uses default.

# --- Local Temporary File Paths ---
LOCAL_CSV_PATH = "temp_downloaded_stock_summary.csv"  # Temporary local path for CSV

# --- Pillow Drawing Configuration (Copied from previous script) ---
OUTPUT_IMAGE_PATH_LOCAL_DEBUG = (
    "stock_summary_generated_image_local_debug.png"  # For local saving if needed
)

# Colors (RGB)
COLOR_BLACK = (0, 0, 0)
COLOR_WHITE = (255, 255, 255)
COLOR_BACKGROUND = (230, 230, 230)
COLOR_RED_TEXT = (192, 0, 0)
COLOR_GREEN_TEXT = (0, 128, 0)
COLOR_YELLOW_TEXT = (192, 142, 0)  # Dark Yellow for 'no change' count in CP
COLOR_TITLE_BG = (255, 230, 153)
COLOR_TITLE_TEXT = (0, 0, 0)  # Note: (0,0,0) is valid, (0, 0, 0) is more common style
COLOR_TABLE_HEADER_BG = (247, 150, 70)
COLOR_TABLE_HEADER_TEXT = (255, 255, 255)


# Fonts
try:
    FONT_TITLE = ImageFont.truetype(
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf", 22
    )
    FONT_SECTION_LABEL = ImageFont.truetype(
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf", 15
    )
    FONT_TABLE_HEADER = ImageFont.truetype(
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf", 15
    )
    FONT_REGULAR = ImageFont.truetype(
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf", 14
    )
    FONT_SMALL_CP = ImageFont.truetype(
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf", 14
    )
except IOError:
    print(
        "/usr/share/fonts/truetype/freefont/FreeMono.ttf fonts not found. Using default PIL font. Appearance may vary."
    )
    FONT_TITLE = ImageFont.load_default()
    FONT_SECTION_LABEL = ImageFont.load_default()
    FONT_TABLE_HEADER = ImageFont.load_default()
    FONT_REGULAR = ImageFont.load_default()
    FONT_SMALL_CP = ImageFont.load_default()

# Image Dimensions and Layout
IMG_WIDTH = 900
IMG_HEIGHT_ESTIMATE = 550  # Initial estimate, will be adjusted
PADDING = 15
LINE_SPACING = 7  # Extra space between lines of text elements
TITLE_BAR_SPACING_AFTER = PADDING * 0.6  # Specific space after the main title bar
HEADER_BAR_SPACING_AFTER = 5  # Specific space after the table header bar


def get_text_height(font, text="Tg"):
    try:
        return font.getbbox(text)[3] - font.getbbox(text)[1]
    except AttributeError:
        return font.getsize(text)[1]


LINE_HEIGHT_TITLE = get_text_height(FONT_TITLE) + LINE_SPACING + 5
LINE_HEIGHT_TABLE_HEADER = get_text_height(FONT_TABLE_HEADER) + LINE_SPACING + 2
LINE_HEIGHT_REGULAR = get_text_height(FONT_REGULAR) + LINE_SPACING
LINE_HEIGHT_SECTION_LABEL = get_text_height(FONT_SECTION_LABEL) + LINE_SPACING

# CSV Column Indices (important for linking CHỈ SỐ color to (+/-) value)
# Based on the image headers:
# CHỈ SỐ, ĐIỂM, (+/-), (+/- %), KLGD (triệu cp), GTGD (tỷ), CP tăng/giảm
COL_IDX_CHI_SO = 0
COL_IDX_DIEM = 1
COL_IDX_PLUS_MINUS = 2  # This is the column we'll use for coloring CHỈ SỐ
COL_IDX_KLGD = 3
COL_IDX_GTGD = 4
COL_IDX_CP_TANG_GIAM = 5


# Column X positions (start of each column) - Tweak these for precise alignment
COL_X = [
    PADDING,
    PADDING + 100,
    PADDING + 200,
    PADDING + 275,
    PADDING + 410,
    PADDING + 520,
    PADDING + 650,
]
COL_WIDTHS = [(COL_X[i + 1] - COL_X[i] - 5) for i in range(len(COL_X) - 1)]
COL_WIDTHS.append(IMG_WIDTH - COL_X[-1] - PADDING)


# --- S3 Helper Functions ---
def get_s3_client(region_name=None):
    """Initializes and returns an S3 client."""
    try:
        if region_name:
            return boto3.client(
                "s3",
                region_name=region_name,
                aws_access_key_id="",
                aws_secret_access_key="",
            )
        else:
            return boto3.client(
                "s3",
                aws_access_key_id="",
                aws_secret_access_key="",
            )
    except NoCredentialsError:
        print(
            "AWS credentials not found. Configure AWS credentials (e.g., environment variables, ~/.aws/credentials, or IAM role)."
        )
        return None


def download_from_s3(s3_client, bucket, key, local_path):
    if not s3_client:
        return False
    try:
        s3_client.download_file(bucket, key, local_path)
        print(f"DL OK: s3://{bucket}/{key}")
        return True
    except ClientError as e:
        print(f"DL S3 Error: {e} for s3://{bucket}/{key}")
        return False
    except Exception as e:
        print(f"DL Other Error: {e}")
        return False


def upload_fileobj_to_s3(s3_client, f_obj, bucket, key, args=None):
    if not s3_client:
        return False
    try:
        f_obj.seek(0)
        s3_client.upload_fileobj(f_obj, bucket, key, ExtraArgs=args)
        print(f"UL OK: s3://{bucket}/{key}")
        return True
    except ClientError as e:
        print(f"UL S3 Error: {e} for s3://{bucket}/{key}")
        return False
    except Exception as e:
        print(f"UL Other Error: {e}")
        return False


# --- Pillow Drawing Helper Functions ---
def get_color_from_change_value(change_text_value):
    """Determines color based on a change value string (like from '+/-' column)."""
    text_str = str(change_text_value)
    if "↑" in text_str or (text_str.startswith("+") and not text_str.startswith("+-")):
        return COLOR_GREEN_TEXT
    if "↓" in text_str:
        return COLOR_RED_TEXT
    cleaned_for_negative_check = (
        text_str.replace("↓", "").replace("(", "").replace(")", "").replace(" ", "")
    )
    if cleaned_for_negative_check.startswith("-"):
        return COLOR_RED_TEXT
    return COLOR_BLACK  # Default if no clear +/- indication (e.g., "0.00" or empty)


def draw_aligned_text(
    draw, x_start, y_center, text, font, color, col_width, align="left"
):
    text_bbox = draw.textbbox((0, 0), text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    actual_y = y_center - text_height / 2
    if align == "right":
        actual_x = x_start + col_width - text_width - PADDING * 0.2
    elif align == "center":
        actual_x = x_start + (col_width - text_width) / 2
    else:
        actual_x = x_start + PADDING * 0.2
    draw.text((actual_x, actual_y), text, font=font, fill=color)


def parse_cp_adv_dec_nc(cp_string):
    parts = cp_string.split("|")
    parsed = {"adv": None, "nc": None, "dec": None}
    for part in parts:
        if "↑" in part:
            parsed["adv"] = part.replace("↑", "").strip()
        elif "↔" in part:
            parsed["nc"] = part.replace("↔", "").strip()
        elif "↓" in part:
            parsed["dec"] = part.replace("↓", "").strip()
    return parsed


# --- Main Script ---
def main():
    if S3_BUCKET_NAME == "your-s3-bucket-name":
        print(
            "Please configure S3_BUCKET_NAME, S3_CSV_KEY, and S3_IMAGE_KEY in the script."
        )
        return

    s3 = get_s3_client(S3_REGION)
    if not s3:
        return

    if not download_from_s3(s3, S3_BUCKET_NAME, S3_CSV_KEY, LOCAL_CSV_PATH):
        return

    all_csv_rows = []
    try:
        with open(LOCAL_CSV_PATH, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            all_csv_rows = [
                row for row in reader if any(field.strip() for field in row)
            ]
    except Exception as e:
        print(f"Error reading local CSV '{LOCAL_CSV_PATH}': {e}")
        return
    finally:
        if os.path.exists(LOCAL_CSV_PATH):
            try:
                os.remove(
                    LOCAL_CSV_PATH
                )  # print(f"Temp CSV '{LOCAL_CSV_PATH}' deleted.")
            except OSError as e:
                print(f"Error deleting temp CSV: {e}")

    if not all_csv_rows:
        print("CSV data is empty.")
        return
    print("all_csv_rows", all_csv_rows)

    img = Image.new("RGB", (IMG_WIDTH, IMG_HEIGHT_ESTIMATE), COLOR_BACKGROUND)
    draw = ImageDraw.Draw(img)
    current_y = 0

    # --- Pillow drawing logic (REVISED) ---
    # 1. Draw Title
    title_text = all_csv_rows[0][0]
    title_bar_height = LINE_HEIGHT_TITLE  # Height of the colored bar for the title
    draw.rectangle(
        [(0, current_y), (IMG_WIDTH, current_y + title_bar_height)], fill=COLOR_TITLE_BG
    )
    draw_aligned_text(
        draw,
        0,
        current_y + title_bar_height / 2,
        title_text,
        FONT_TITLE,
        COLOR_TITLE_TEXT,
        IMG_WIDTH,
        "center",
    )
    current_y += (
        title_bar_height + TITLE_BAR_SPACING_AFTER
    )  # Ensure distinct spacing after title bar

    # Find table headers
    header_row_idx = -1
    for i, row in enumerate(all_csv_rows):
        if row and row[0].strip() == "CHỈ SỐ":
            header_row_idx = i
            break
    if header_row_idx == -1:
        print("Table headers 'CHỈ SỐ' not found in CSV.")
        return

    # 2. Draw Table Headers
    table_headers = all_csv_rows[header_row_idx]
    header_bar_height = (
        LINE_HEIGHT_TABLE_HEADER  # Height of the colored bar for headers
    )
    draw.rectangle(
        [(0, current_y), (IMG_WIDTH, current_y + header_bar_height)],
        fill=COLOR_TABLE_HEADER_BG,
    )
    header_text_y_center = current_y + header_bar_height / 2
    for i, header_text in enumerate(table_headers):
        if i < len(COL_X) and i < len(COL_WIDTHS):
            # Center header text within its column boundaries
            text_x_start = COL_X[i]
            text_col_width = COL_WIDTHS[i]
            # For header, actual text alignment within the cell is usually centered by default by draw_aligned_text
            draw_aligned_text(
                draw,
                text_x_start,
                header_text_y_center,
                header_text,
                FONT_TABLE_HEADER,
                COLOR_TABLE_HEADER_TEXT,
                text_col_width,
                "center",
            )
    current_y += (
        header_bar_height + HEADER_BAR_SPACING_AFTER
    )  # Ensure distinct spacing after header bar

    # 3. Draw Index Data Rows (MODIFIED SECTION)
    idx_data_start = header_row_idx + 1
    idx_data_end = idx_data_start
    for i in range(idx_data_start, len(all_csv_rows)):
        if (
            not all_csv_rows[i]
            or (all_csv_rows[i][0].strip().startswith("Khối ngoại:"))
            or not all_csv_rows[i][0].strip()
        ):
            idx_data_end = i
            break
        idx_data_end = i + 1

    row_actual_height = LINE_HEIGHT_REGULAR
    for r_idx in range(idx_data_start, idx_data_end):
        row_data = all_csv_rows[r_idx]
        if (
            not row_data or len(row_data) <= COL_IDX_PLUS_MINUS
        ):  # Ensure row has enough columns
            current_y += row_actual_height  # Still advance y
            continue

        data_text_y_center = current_y + row_actual_height / 2

        # Determine color for CHỈ SỐ based on the (+/-) column value of the current row
        plus_minus_value_for_coloring = row_data[COL_IDX_PLUS_MINUS]
        chi_so_color = get_color_from_change_value(plus_minus_value_for_coloring)

        for c_idx, cell_text in enumerate(row_data):
            if c_idx >= len(COL_X) or c_idx >= len(COL_WIDTHS):
                continue

            align = "left"
            cell_color = COLOR_BLACK  # Default
            font_to_use = FONT_REGULAR

            if c_idx == COL_IDX_DIEM:  # ĐIỂM
                align = "right"
                cell_color = chi_so_color  # Use same color as CHỈ SỐ / (+/-)
            elif c_idx == COL_IDX_PLUS_MINUS:  # (+/-) and (+/- %)
                align = "center"
                cell_color = get_color_from_change_value(cell_text)
            elif c_idx == COL_IDX_KLGD or c_idx == COL_IDX_GTGD:  # KLGD, GTGD
                align = "right"
                cell_color = cell_color
            elif c_idx == COL_IDX_CP_TANG_GIAM:  # CP tăng/giảm
                cp_data = parse_cp_adv_dec_nc(cell_text)
                cp_x_start = COL_X[c_idx]
                cp_col_width = COL_WIDTHS[c_idx]
                items_to_draw_specs = []
                if cp_data["adv"]:
                    items_to_draw_specs.append(
                        {"text": "↑" + cp_data["adv"], "color": COLOR_GREEN_TEXT}
                    )
                if cp_data["nc"]:
                    items_to_draw_specs.append(
                        {"text": "↔" + cp_data["nc"], "color": COLOR_YELLOW_TEXT}
                    )
                if cp_data["dec"]:
                    items_to_draw_specs.append(
                        {"text": "↓" + cp_data["dec"], "color": COLOR_RED_TEXT}
                    )

                total_items_width = 0
                spacing_between_items = 10
                for item_idx, item_spec in enumerate(items_to_draw_specs):
                    item_bbox = draw.textbbox(
                        (0, 0), item_spec["text"], font=FONT_SMALL_CP
                    )
                    total_items_width += item_bbox[2] - item_bbox[0]
                    if item_idx < len(items_to_draw_specs) - 1:
                        total_items_width += spacing_between_items
                current_item_x = cp_x_start + (cp_col_width - total_items_width) / 2
                for item_spec in items_to_draw_specs:
                    item_bbox = draw.textbbox(
                        (0, 0), item_spec["text"], font=FONT_SMALL_CP
                    )
                    item_width = item_bbox[2] - item_bbox[0]
                    draw_aligned_text(
                        draw,
                        current_item_x,
                        data_text_y_center,
                        item_spec["text"],
                        FONT_SMALL_CP,
                        item_spec["color"],
                        item_width,
                        "left",
                    )
                    current_item_x += item_width + spacing_between_items
                continue  # Skip the generic draw_aligned_text for this specific column

            draw_aligned_text(
                draw,
                COL_X[c_idx],
                data_text_y_center,
                cell_text,
                font_to_use,
                cell_color,
                COL_WIDTHS[c_idx],
                align,
            )
        current_y += row_actual_height

    # 4. Draw Other Sections (Assumed mostly correct from previous version)
    current_y += PADDING * 0.7
    section_row_height = LINE_HEIGHT_SECTION_LABEL
    for r_idx in range(idx_data_end, len(all_csv_rows)):
        row_data = all_csv_rows[r_idx]
        if not row_data or not row_data[0].strip():
            current_y += LINE_SPACING / 2
            continue
        label = row_data[0]
        values = row_data[1:]
        section_text_y_center = current_y + section_row_height / 2
        label_bbox = draw.textbbox((0, 0), label, font=FONT_SECTION_LABEL)
        draw_aligned_text(
            draw,
            PADDING,
            section_text_y_center,
            label,
            FONT_SECTION_LABEL,
            COLOR_BLACK,
            label_bbox[2] - label_bbox[0] + PADDING * 0.4,
            "left",
        )
        current_val_x = PADDING + (label_bbox[2] - label_bbox[0]) + PADDING * 0.5
        if label.startswith("Khối ngoại:"):
            if len(values) > 0 and values[0].strip():
                vol_text = values[0]
                vol_color = get_color_from_change_value(
                    vol_text
                )  # Use change logic for Khoi Ngoai Vol
                vol_bbox = draw.textbbox((0, 0), vol_text, font=FONT_REGULAR)
                draw_aligned_text(
                    draw,
                    current_val_x,
                    section_text_y_center,
                    vol_text,
                    FONT_REGULAR,
                    vol_color,
                    vol_bbox[2] - vol_bbox[0],
                    "left",
                )
            if len(values) > 2 and values[2].strip():
                net_text = values[2]
                net_color = get_color_from_change_value(
                    net_text
                )  # Use change logic for Khoi Ngoai Net
                net_val_x_align = COL_X[4]
                net_bbox = draw.textbbox((0, 0), net_text, font=FONT_REGULAR)
                draw_aligned_text(
                    draw,
                    net_val_x_align,
                    section_text_y_center,
                    net_text,
                    FONT_REGULAR,
                    net_color,
                    net_bbox[2] - net_bbox[0],
                    "left",
                )
        elif values and values[0].strip():
            val_text = values[0]
            val_color = COLOR_BLACK
            if label.startswith("Khối tự doanh:"):
                val_color = get_color_from_change_value(val_text)  # Use change logic
            max_list_width = IMG_WIDTH - current_val_x - PADDING
            words = val_text.split(", ")
            line_to_draw = ""
            temp_y_offset = 0
            lines_drawn = 0
            for i, word in enumerate(words):
                separator = "" if not line_to_draw else ", "
                test_line = line_to_draw + separator + word
                if (
                    draw.textbbox((0, 0), test_line, font=FONT_REGULAR)[2]
                    <= max_list_width
                ):
                    line_to_draw = test_line
                else:
                    draw_aligned_text(
                        draw,
                        current_val_x,
                        section_text_y_center + temp_y_offset,
                        line_to_draw,
                        FONT_REGULAR,
                        val_color,
                        max_list_width,
                        "left",
                    )
                    temp_y_offset += get_text_height(FONT_REGULAR) + LINE_SPACING * 0.5
                    line_to_draw = word
                    lines_drawn += 1
            if line_to_draw:
                draw_aligned_text(
                    draw,
                    current_val_x,
                    section_text_y_center + temp_y_offset,
                    line_to_draw,
                    FONT_REGULAR,
                    val_color,
                    max_list_width,
                    "left",
                )
                lines_drawn += 1
            current_y += temp_y_offset if lines_drawn > 1 else 0
        current_y += section_row_height
    # --- End of Pillow drawing logic ---

    current_y += PADDING  # Final padding
    img_final_height = int(current_y)
    final_image = img.crop((0, 0, IMG_WIDTH, img_final_height))

    # Optional: Save image locally for debugging
    try:
        final_image.save(OUTPUT_IMAGE_PATH_LOCAL_DEBUG)
        print(f"Image '{OUTPUT_IMAGE_PATH_LOCAL_DEBUG}' saved locally for debugging.")
    except Exception as e:
        print(f"Error saving local debug image: {e}")

    image_buffer = io.BytesIO()
    final_image.save(image_buffer, format="PNG")
    if not upload_fileobj_to_s3(
        s3,
        image_buffer,
        S3_BUCKET_NAME,
        S3_IMAGE_KEY,
        args={"ContentType": "image/png"},
    ):
        print("Failed to upload image to S3.")
