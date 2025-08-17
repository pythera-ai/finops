import boto3
import os
import tempfile
from datetime import datetime
from typing import Dict, Any
from jinja2 import Environment, FileSystemLoader, select_autoescape

# --- UPDATED AND CORRECTED HTML TEMPLATE ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Times+New+Roman&display=swap');
        body { font-family: 'Times New Roman', Times, serif; background-color: #ffffff; margin: 0; padding: 0; }
        .report-container { border: 1px solid #ccc; width: 800px; margin: 20px auto; }
        .title { background-color: #4CAF50; color: #FF0000; font-size: 24px; font-weight: bold; text-align: center; padding: 15px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ccc; padding: 6px 8px; font-size: 13px; text-align: right; vertical-align: middle; }
        th { background-color: #FAC090; font-weight: bold; text-align: center; }
        tr:nth-child(even) { background-color: #F0F0F0; }
        .index-name { text-align: left; font-weight: bold; }
        .increase { color: #008000; font-weight: bold; }
        .decrease { color: #FF0000; font-weight: bold; }
        .neutral { color: #000000; }
        .yellow { color: #FFD700; font-weight: bold; }
        .section { background-color: #E8E8E8; padding: 8px; margin-top: -1px; border: 1px solid #ccc; font-size: 13px; }
        .section-title { font-weight: bold; }
    </style>
</head>
<body>
    <div class="report-container">
        <div class="title">TỔNG KẾT GIAO DỊCH PHIÊN {{ report_date }}</div>
        <table>
            <thead>
                <tr>
                    <th>CHỈ SỐ</th><th>ĐIỂM</th><th>(+/-)</th><th>(+/- %)</th><th>KLGD(triệu cp)</th><th>(+/-%KLGD)</th><th>GTGD (tỷ)</th><th>(+/-%GTGD)</th><th>CP tăng/giảm</th>
                </tr>
            </thead>
            <tbody>
            {% for item in ordered_summary %}
                <tr class="{% if item.change > 0 %}increase{% elif item.change < 0 %}decrease{% else %}neutral{% endif %}">
                    <td class="index-name">{{ item.display_name }}</td>
                    <td>{{ "%.2f"|format(item.indexValue) }}</td>
                    <td class="{% if item.change > 0 %}increase{% elif item.change < 0 %}decrease{% endif %}">{% if item.change > 0 %}↑{% else %}↓{% endif %}{{ "%.2f"|format(item.change|abs) }}</td>
                    <td class="{% if item.changePercent > 0 %}increase{% elif item.changePercent < 0 %}decrease{% endif %}">{% if item.changePercent > 0 %}↑{% else %}↓{% endif %}{{ "%.2f"|format(item.changePercent|abs) }}</td>
                    <td class="neutral">{{ "%.2f"|format(item.allQty) }}</td>
                    
                    <!-- FIX APPLIED HERE: Using default(0) -->
                    <td class="{% if item.klgd_change_percent|default(0) > 0 %}increase{% elif item.klgd_change_percent|default(0) < 0 %}decrease{% endif %}">
                        {% set klgd_change = item.klgd_change_percent|default(0) %}
                        {% if klgd_change != 0 %}{% if klgd_change > 0 %}↑{% else %}↓{% endif %}{{ "%.2f"|format(klgd_change|abs) }}{% else %}↔0.00{% endif %}
                    </td>
                    
                    <td class="neutral">{{ "%.2f"|format(item.allValue) }}</td>
                    
                    <!-- FIX APPLIED HERE: Using default(0) -->
                    <td class="{% if item.gtdg_change_percent|default(0) > 0 %}increase{% elif item.gtdg_change_percent|default(0) < 0 %}decrease{% endif %}">
                        {% set gtdg_change = item.gtdg_change_percent|default(0) %}
                        {% if gtdg_change != 0 %}{% if gtdg_change > 0 %}↑{% else %}↓{% endif %}{{ "%.2f"|format(gtdg_change|abs) }}{% else %}↔0.00{% endif %}
                    </td>
                    
                    <td style="text-align:center;"><span class="increase">↑{{ item.advances }}</span>|<span class="yellow">↔{{ item.nochanges }}</span>|<span class="decrease">↓{{ item.declines }}</span></td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
        <div class="section"><span class="section-title {% if khoi_ngoai.vol > 0 %}increase{% elif khoi_ngoai.vol < 0 %}decrease{% endif %}">Khối ngoại: {% if khoi_ngoai.vol > 0 %}↑{{ "%.2f"|format(khoi_ngoai.vol) }}{% else %}↓{{ "%.2f"|format(khoi_ngoai.vol|abs) }}{% endif %} triệu cp</span> <span class="section-title {% if khoi_ngoai.net_value > 0 %}increase{% elif khoi_ngoai.net_value < 0 %}decrease{% endif %}">{% if khoi_ngoai.net_value > 0 %}↑{{ "%.2f"|format(khoi_ngoai.net_value) }}{% else %}↓{{ "%.2f"|format(khoi_ngoai.net_value|abs) }}{% endif %} tỷ đồng</span></div>
        <div class="section"><span class="section-title increase">Top mua ròng: </span><span style="color: #000000;">{{ top_netforeign.buy | join(', ') }}</span></div>
        <div class="section"><span class="section-title decrease">Top bán ròng: </span><span style="color: #000000;">{{ top_netforeign.sell | join(', ') }}</span></div>
        <div class="section"><span class="section-title {% if khoi_tu_doanh > 0 %}increase{% elif khoi_tu_doanh < 0 %}decrease{% endif %}">Khối tự doanh: {% if khoi_tu_doanh > 0 %}↑{{ "%.0f"|format(khoi_tu_doanh) }}{% else %}↓{{ "%.0f"|format(khoi_tu_doanh|abs) }}{% endif %} tỷ đồng</span></div>
        <div class="section"><span class="section-title">Nhóm ngành nổi bật: </span>{{ top_sectors | join(', ') }}</div>
        <div class="section"><span class="section-title">Cổ phiếu tâm điểm: </span>{{ top_interested | join(', ') }}</div>
        <div class="section"><span class="section-title increase">Tác động tăng (+{{ "%.2f"|format(impact_up.total) }}): </span><span class="increase">{{ impact_up.stock_code | join(', ') }}</span></div>
        <div class="section"><span class="section-title decrease">Tác động giảm ({{ "%.2f"|format(impact_down.total) }}): </span><span class="decrease">{{ impact_down.stock_code | join(', ') }}</span></div>
    </div>
</body>
</html>
"""


def generate_html_report_content(data: Dict[str, Any]) -> str:
    """Renders the data into the HTML template."""
    from jinja2 import Template

    template = Template(HTML_TEMPLATE)

    # Prepare data for template
    index_mapping = {
        "VNINDEX": "VNINDEX",
        "VN30": "VN30",
        "HNXIndex": "HNXINDEX",
        "HNX30": "HNX30",
        "HNXUpcomIndex": "UPCOM",
    }
    index_data_by_id = {item["indexId"]: item for item in data.get("index_summary", [])}

    ordered_summary = []
    for idx, name in index_mapping.items():
        if idx in index_data_by_id:
            item = index_data_by_id[idx]
            item["display_name"] = name
            ordered_summary.append(item)

    template_data = {
        "report_date": datetime.now().strftime("%d/%m/%Y"),
        "ordered_summary": ordered_summary,
        **data,
    }

    return template.render(template_data)


def main(
    data: Dict[str, Any],
    s3_bucket_name: str,
    s3_html_key: str,
    s3_region: str = "ap-southeast-2",
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
):
    """Generates an HTML report and uploads it to S3."""
    try:
        html_content = generate_html_report_content(data)

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".html", delete=False, encoding="utf-8"
        ) as temp_file:
            temp_file.write(html_content)
            temp_file_path = temp_file.name

        s3_client = boto3.client(
            "s3",
            region_name=s3_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3_client.upload_file(
            temp_file_path,
            s3_bucket_name,
            s3_html_key,
            ExtraArgs={"ContentType": "text/html"},
        )
        file_size = os.path.getsize(temp_file_path)
        os.unlink(temp_file_path)

        return {
            "success": True,
            "message": "HTML report generated and uploaded successfully.",
            "s3_html_location": f"s3://{s3_bucket_name}/{s3_html_key}",
            "file_size_bytes": file_size,
        }
    except Exception as e:
        import traceback

        print(f"Error during HTML generation: {e}")
        print(traceback.format_exc())
        return {"success": False, "message": f"HTML generation failed: {str(e)}"}
