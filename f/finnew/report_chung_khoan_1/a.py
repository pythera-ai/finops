# import wmill
from jinja2 import Template
import json
from datetime import datetime

def merge_inputs(args):
    merged = {}

    for data in args:
        print("data: ", data)
        if not data:
            continue
        # data = json.loads(data)
        if "impact_up" in data:
            merged["impact_up"] = data["impact_up"]
        if "impact_down" in data:
            merged["impact_down"] = data["impact_down"]
        if "index_summary" in data:
            merged["index_summary"] = []
            for e in data["index_summary"]:
                e["allQty"] = e["allQty"] / 10**6
                e["allValue"] = e["allValue"] / 10**9
                merged["index_summary"].append(e)
        if "khoi_ngoai" in data:
            merged["khoi_ngoai"] = data["khoi_ngoai"]
        if "top_interested" in data:
            merged["top_interested"] = data["top_interested"]
        if "top_netforeign" in data:
            merged["top_netforeign"] = data["top_netforeign"]
        if "khoi_tu_doanh" in data:
            merged["khoi_tu_doanh"] = data["khoi_tu_doanh"]
        if "top_sectors" in data:
            merged["top_sectors"] = data["top_sectors"]
    return merged


def main(args: list):
    #
    merged_data = merge_inputs(args)
    print(merged_data)

    # 
    report_date = datetime.now().strftime("%d/%m/%Y")

    # Step 2: Jinja2 Template
    template_str = """
# 📊 TỔNG KẾT GIAO DỊCH NGÀY {{ report_date }}

| CHỈ SỐ | ĐIỂM | (+/-) | KLGD (triệu cp) | GTGD (tỷ đồng) | CP Tăng / Giảm |
| --- | --- | --- | --- | --- | --- |
{% for idx in index_summary -%}
| {{ idx.indexId }} | {{ "%.2f"|format(idx.indexValue) }} | {%- if idx.change > 0 %} 🔼 {{ "%.2f"|format(idx.change) }} {%- elif idx.change < 0 %} 🔽 {{ "%.2f"|format(idx.change) }} {%- else %} {{ "%.2f"|format(idx.change) }}{%- endif %} | {{ "%.3f"|format(idx.allQty) }} | {{ "%.3f"|format(idx.allValue) }} | {{ idx.advances }}\|{{ idx.nochanges }}\|{{ idx.declines }} |
{% endfor %}

## 💡 Thông tin thêm

- **🌐 Khối ngoại**: {{ "%.2f"|format(khoi_ngoai.vol) }} triệu cp / {{ "%.2f"|format(khoi_ngoai.net_value) }} tỷ đồng
- **🧑‍💼 Khối tự doanh**: {{ "%.2f"|format(khoi_tu_doanh) }} tỷ đồng
- **📥 Top mua ròng khối ngoại**: {{ top_netforeign.buy | join(", ") }}
- **📤 Top bán ròng khối ngoại**: {{ top_netforeign.sell | join(", ") }}
- **👀 Cổ phiếu được quan tâm**: {{ top_interested | join(", ") }}
- **🏭 Ngành nổi bật**: {{ top_sectors | join(", ") }}

"""

    # Step 3: Render the template
    template = Template(template_str)
    rendered_markdown = template.render(**merged_data)

    return rendered_markdown
