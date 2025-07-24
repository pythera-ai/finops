import logging
from typing import Dict, Any


def main(raw_market_data: Dict[str, Any], analysis_target_index: str) -> Dict[str, Any]:
    """
    Filters the full market data based on the user's selected index.
    This prepares a targeted dataset for the analysis and reporting steps.
    """
    try:
        if not raw_market_data or not raw_market_data.get("success"):
            raise ValueError("Invalid raw market data received.")

        full_data = raw_market_data.get("data", {})
        all_indices = full_data.get("market_indices", {})

        if not all_indices:
            raise ValueError("No market indices found in the parsed data.")

        analysis_data = {}
        display_name = ""

        if analysis_target_index == "ALL":
            analysis_data = all_indices
            display_name = "Toàn bộ thị trường"
        elif analysis_target_index in all_indices:
            analysis_data = {analysis_target_index: all_indices[analysis_target_index]}
            display_name = analysis_target_index
        else:
            raise ValueError(
                f"Selected index '{analysis_target_index}' not found in the data. Available indices: {list(all_indices.keys())}"
            )

        # Pass through all original data plus the filtered target data
        return {
            "success": True,
            "analysis_data": analysis_data,
            "display_name": display_name,
            "data": full_data,  # Pass the original full dataset for context
        }

    except Exception as e:
        logging.error(f"Error preparing analysis data: {e}")
        return {"success": False, "error": str(e)}
