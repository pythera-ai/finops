import json
from datetime import datetime


def validate_data_quality(merged_data: dict) -> dict:
    quality_report = {
        "passed": True,
        "issues": [],
        "warnings": [],
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Check for empty critical data
    if not merged_data.get("index_summary"):
        quality_report["issues"].append("Missing index summary data")
        quality_report["passed"] = False

    if not merged_data.get("khoi_ngoai"):
        quality_report["warnings"].append("Missing khoi ngoai data")

    # Check for data completeness
    required_fields = [
        "top_sectors",
        "top_netforeign",
        "top_interested",
        "khoi_tu_doanh",
    ]
    for field in required_fields:
        if field not in merged_data or not merged_data[field]:
            quality_report["warnings"].append(f"Missing or empty {field}")

    return quality_report


def merge_inputs(args: list, last_day_data: dict) -> dict:
    merged = {}
    success_count = 0
    total_modules = len(args)
    errors = []

    # --- NEW: Create a lookup map for last day's data for efficiency ---
    last_day_gtdg = last_day_data.get("last_day_gtdg", {})
    last_day_klgd = last_day_data.get("last_day_klgd", {})

    for i, data in enumerate(args):
        if not data:
            continue

        # Track success/failure
        if data.get("success", True):
            success_count += 1
        else:
            errors.append(f"Module {i}: {data.get('error', 'Unknown error')}")

        # Merge data fields
        data_mappings = {
            "impact_up": "impact_up",
            "impact_down": "impact_down",
            "index_summary": "index_summary",
            "khoi_ngoai": "khoi_ngoai",
            "top_interested": "top_interested",
            "top_netforeign": "top_netforeign",
            "khoi_tu_doanh": "khoi_tu_doanh",
            "top_sectors": "top_sectors",
        }

        for source_key, target_key in data_mappings.items():
            if source_key in data:
                if source_key == "index_summary" and data[source_key]:
                    # Process index summary to convert units
                    processed_summary = []
                    for item in data[source_key]:
                        item_copy = item.copy()
                        # --- NEW: Add last day's GTGD to each index item ---
                        index_id = item_copy.get("indexId")
                        last_day_gtdg_value = float(last_day_gtdg.get(index_id, 0))
                        last_day_klgd_value = float(last_day_klgd.get(index_id, 0))

                        # Convert units (from base units to billions)
                        item_copy["gtdg_last_day"] = last_day_gtdg_value
                        item_copy["klgd_last_day"] = last_day_klgd_value

                        # --- END OF NEW LOGIC ---

                        if "allQty" in item_copy and item_copy["allQty"] != "N/A":
                            try:
                                current_klgd_millions = (
                                    float(item_copy["allQty"]) / 10**6
                                )
                                item_copy["allQty"] = current_klgd_millions
                                # Calculate percentage change
                                klgd_change_percent = (
                                    (current_klgd_millions - last_day_klgd_value)
                                    / last_day_klgd_value
                                    * 100
                                )
                                item_copy["klgd_change_percent"] = round(
                                    klgd_change_percent, 2
                                )
                                # Add absolute change amount
                                item_copy["klgd_change_amount"] = round(
                                    current_klgd_millions - last_day_klgd_value, 3
                                )
                            except:
                                pass
                        if "allValue" in item_copy and item_copy["allValue"] != "N/A":
                            try:
                                current_gtdg_billions = (
                                    float(item_copy["allValue"]) / 10**9
                                )
                                item_copy["allValue"] = current_gtdg_billions
                                # Calculate percentage change
                                gtdg_change_percent = (
                                    (current_gtdg_billions - last_day_gtdg_value)
                                    / last_day_gtdg_value
                                    * 100
                                )
                                item_copy["gtdg_change_percent"] = round(
                                    gtdg_change_percent, 2
                                )
                                # Add absolute change amount
                                item_copy["gtdg_change_amount"] = round(
                                    current_gtdg_billions - last_day_gtdg_value, 3
                                )

                            except:
                                pass
                        processed_summary.append(item_copy)
                    merged[target_key] = processed_summary
                else:
                    merged[target_key] = data[source_key]

    # Add execution metadata
    merged["execution_metadata"] = {
        "success_rate": success_count / total_modules if total_modules > 0 else 0,
        "successful_modules": success_count,
        "total_modules": total_modules,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Run data quality validation
    quality_report = validate_data_quality(merged)
    merged["data_quality"] = quality_report

    return merged


def main(args: list, last_day_data: dict):
    merged_data = merge_inputs(args, last_day_data)

    # Log execution summary
    metadata = merged_data.get("execution_metadata", {})
    quality = merged_data.get("data_quality", {})

    print(
        f"Data merge completed: {metadata.get('success_rate', 0) * 100:.1f}% success rate"
    )
    print(f"Data quality: {'PASSED' if quality.get('passed', False) else 'FAILED'}")

    if quality.get("issues"):
        print(f"Quality issues: {', '.join(quality['issues'])}")
    if quality.get("warnings"):
        print(f"Quality warnings: {', '.join(quality['warnings'])}")

    return merged_data
