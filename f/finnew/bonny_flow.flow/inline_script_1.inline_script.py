import json
from datetime import datetime

def main(
    validation_result: dict,
    health_check_result: dict,
    merge_result: dict,
    report_result: dict = None
):
    # Compile final status report
    final_report = {
        "workflow_status": "COMPLETED",
        "timestamp": datetime.utcnow().isoformat(),
        "execution_summary": {
            "input_validation": validation_result.get("validation_passed", False),
            "api_health": health_check_result.get("healthy", False),
            "data_merge_success": merge_result.get("execution_metadata", {}).get("success_rate", 0),
            "data_quality_passed": merge_result.get("data_quality", {}).get("passed", False),
            "report_generated": report_result.get("success", False) if report_result else False
        },
        "performance_metrics": {
            "api_health_percentage": health_check_result.get("health_percentage", 0),
            "data_success_rate": merge_result.get("execution_metadata", {}).get("success_rate", 0) * 100,
            "total_modules_executed": merge_result.get("execution_metadata", {}).get("total_modules", 0),
            "successful_modules": merge_result.get("execution_metadata", {}).get("successful_modules", 0)
        },
        "output_files": [],
        "recommendations": []
    }
    
    # Add output files if report was generated
    if report_result and report_result.get("success"):
        final_report["output_files"].append({
            "type": "CSV",
            "location": report_result.get("s3_location"),
            "size_bytes": report_result.get("file_size_bytes"),
            "rows": report_result.get("rows_generated")
        })
    
    # Generate recommendations based on performance
    if final_report["performance_metrics"]["api_health_percentage"] < 80:
        final_report["recommendations"].append("Consider checking API endpoint health")
    
    if final_report["performance_metrics"]["data_success_rate"] < 80:
        final_report["recommendations"].append("Review data source reliability")
    
    if not final_report["execution_summary"]["data_quality_passed"]:
        final_report["recommendations"].append("Investigate data quality issues")
    
    # Determine overall workflow status
    if final_report["execution_summary"]["report_generated"]:
        final_report["workflow_status"] = "SUCCESS"
    elif final_report["performance_metrics"]["data_success_rate"] >= 50:
        final_report["workflow_status"] = "PARTIAL_SUCCESS"
    else:
        final_report["workflow_status"] = "FAILED"
    
    # Print summary
    print("=" * 60)
    print("WINDMILL WORKFLOW EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Status: {final_report['workflow_status']}")
    print(f"API Health: {final_report['performance_metrics']['api_health_percentage']:.1f}%")
    print(f"Data Success Rate: {final_report['performance_metrics']['data_success_rate']:.1f}%")
    print(f"Report Generated: {'Yes' if final_report['execution_summary']['report_generated'] else 'No'}")
    
    if final_report["recommendations"]:
        print("\nRecommendations:")
        for rec in final_report["recommendations"]:
            print(f"- {rec}")
    
    print("=" * 60)
    
    return final_report
