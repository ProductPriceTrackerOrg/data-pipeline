
from pathlib import Path
import pandas as pd
from prediction_pipeline.pipeline import run_prediction_pipeline
from big_query.data_store import update_categories_in_bigquery
from big_query.data_loading import get_uncategorized_products



def main():
    
    # Replace this with your actual data loading code
    print("Loading data from data warehouse...")
    
    # no nay products to categorize exit
    input_dataframe = get_uncategorized_products()
    print(f"Data loaded. Number of products to categorize: {len(input_dataframe)}")
    if input_dataframe.empty:
        print("No uncategorized products found. Exiting.")
        return

    # Example 2: Run the prediction pipeline
    model_path = Path(__file__).resolve().parent / "models" / "ann_category_classifier.pth"  # keep path stable inside container

    results = run_prediction_pipeline(
        input_df=input_dataframe,
        model_path=str(model_path)
    )

    # product_categorization/models/ann_category_classifier.pth

    # Example 3: Save results
    # print("\nSaving results...")
    # results.to_csv('product_predictions.csv', index=False)
    # print("Results saved to 'product_predictions.csv'")

    # Display final results
    print("\nFINAL RESULTS:")
    print(results)

        # Run the update process
    metrics = update_categories_in_bigquery(results)

    print("\n--- Final Report ---")
    print(f"Successfully Updated: {metrics['updated_count']} products")
    print(f"Product IDs Not Found: {metrics['not_found_count']} products")

if __name__ == "__main__":
    main()