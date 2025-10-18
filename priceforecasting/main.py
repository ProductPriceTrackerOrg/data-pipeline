"""
Production Pipeline Runner
Simple script to execute the complete LSTM price forecasting pipeline
"""

import sys
import os
from datetime import datetime

# Add current directory to path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from production_pipeline import ProductionPipeline
    import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure all required files are in the same directory:")
    print("- production_pipeline.py")
    print("- config.py")
    print("- BigQuery credentials JSON file")
    sys.exit(1)


def run_pipeline():
    """Run the complete production pipeline"""
    print("=" * 80)
    print("🚀 LSTM PRICE FORECASTING PRODUCTION PIPELINE")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Device: {config.DEVICE}")
    print(f"Project: {config.PROJECT_ID}")
    print(f"Dataset: {config.DATASET_ID}")
    print("=" * 80)

    # Create and run pipeline
    pipeline = ProductionPipeline()
    success = pipeline.run_complete_pipeline()

    if success:
        print("\n🎉 Pipeline completed successfully!")
        print("Results:")
        print("- Model fine-tuned on latest BigQuery data")
        print("- 7-day predictions generated for all variants")
        print("- Predictions uploaded to FactPriceForecast table")
        print("- Temporary CSV files cleaned up")
        return 0
    else:
        print("\nFAILED: Pipeline failed!")
        print("Check the logs for detailed error information.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = run_pipeline()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)
