import json
from datetime import datetime

def main(merged_data: dict):
    quality_info = merged_data.get("data_quality", {})
    execution_info = merged_data.get("execution_metadata", {})
    
    failure_report = {
        "status": "FAILED",
        "reason": "Data quality check failed",
        "timestamp": datetime.utcnow().isoformat(),
        "quality_issues": quality_info.get("issues", []),
        "quality_warnings": quality_info.get("warnings", []),
        "success_rate": execution_info.get("success_rate", 0),
        "errors": execution_info.get("errors", [])
    }
    
    print("=" * 50)
    print("REPORT GENERATION FAILED")
    print("=" * 50)
    print(f"Success rate: {failure_report['success_rate']*100:.1f}%")
    print(f"Quality passed: {quality_info.get('passed', False)}")
    
    if failure_report["quality_issues"]:
        print(f"Issues: {', '.join(failure_report['quality_issues'])}")
    
    if failure_report["quality_warnings"]:
        print(f"Warnings: {', '.join(failure_report['quality_warnings'])}")
    
    if failure_report["errors"]:
        print(f"Errors: {', '.join(failure_report['errors'])}")
    
    print("=" * 50)
    
    # Still return the failure report for downstream processing
    return failure_report
