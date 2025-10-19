# LSTM Price Forecasting Production Pipeline

Complete production pipeline that loads a pre-trained LSTM model, fetches data from BigQuery, fine-tunes the model, generates 7-day price forecasts, and uploads results back to BigQuery.

## Pipeline Flow

```
1. Load Pre-trained LSTM Model (lstm_forecaster.pth)
    ↓
2. Fetch BigQuery Data (variant_id, price, date)
    ↓
3. Create CSV File (temporary)
    ↓
4. Fine-tune Model on Latest Data
    ↓
5. Generate 7-Day Predictions
    ↓
6. Upload to FactPriceForecast Table
    ↓
7. Delete CSV File (cleanup)
```

## Quick Start

### Prerequisites

- Python 3.8+
- Pre-trained LSTM model in `models/lstm_forecaster.pth`
- BigQuery service account JSON credentials
- Valid BigQuery dataset with required tables

### Installation

```bash
pip install -r requirements.txt
```

### Run Pipeline

```bash
python run_pipeline.py
```

## Configuration

Edit `config.py` to customize settings:

```python
# BigQuery Settings
PROJECT_ID = "your-project-id"
DATASET_ID = "your-dataset-id"
CREDENTIALS_PATH = "your-credentials.json"

# Model Settings
SEQUENCE_LENGTH = 30      # Days of history for prediction
FORECAST_LENGTH = 7       # Days to predict forward
HIDDEN_SIZE = 128         # LSTM hidden units
NUM_LAYERS = 2           # LSTM layers
BATCH_SIZE = 64          # Training batch size
FINE_TUNE_EPOCHS = 10    # Fine-tuning epochs
```

## BigQuery Schema Requirements

### Input Tables

**FactProductPrice**

- `variant_id` (INT64): Product variant identifier
- `current_price` (FLOAT64): Current price
- `date_id` (INT64): Date key
- `is_available` (BOOL): Product availability

**DimVariant**

- `variant_id` (INT64): Primary key
- `variant_title` (STRING): Variant name

**DimDate**

- `date_id` (INT64): Primary key
- `full_date` (DATE): Actual date

### Output Table

**FactPriceForecast**

- `variant_id` (INT64): Product variant
- `model_id` (INT64): Model identifier (100)
- `forecast_date` (DATE): Prediction date
- `predicted_price` (FLOAT64): Forecasted price
- `confidence_upper` (FLOAT64): Upper confidence bound
- `confidence_lower` (FLOAT64): Lower confidence bound
- `created_at` (DATETIME): Prediction timestamp

**DimModel**

- `model_id` (INT64): Primary key (100)
- `model_name` (STRING): "LSTM_Price_Forecaster"
- `model_version` (STRING): "v1.0_finetuned"
- `model_type` (STRING): "LSTM_Seq2Seq"
- `training_date` (DATE): Fine-tuning date
- `performance_metrics_json` (STRING): JSON metrics
- `hyperparameters_json` (STRING): JSON parameters

## Features

### ✨ Key Capabilities

- **Pre-trained Model Loading**: Loads existing LSTM model from disk
- **Real-time Data Fetching**: Pulls latest 60 days of price data
- **Per-Product Scaling**: Individual normalization for each variant
- **Model Fine-tuning**: Adapts to latest market conditions
- **7-Day Forecasting**: Generates week-ahead predictions
- **Confidence Intervals**: Provides prediction uncertainty bounds
- **Automated Upload**: Saves results directly to BigQuery
- **Clean Architecture**: Modular, maintainable code structure

### 🔧 Technical Features

- **GPU Acceleration**: Automatic CUDA detection and usage
- **Batch Processing**: Efficient handling of large datasets
- **Error Handling**: Comprehensive logging and error recovery
- **Memory Optimization**: Efficient data loading and processing
- **Scalable Design**: Handles thousands of product variants

## File Structure (Modular Architecture)

```
Price Forecasting/
├── production_pipeline.py         # Main pipeline orchestrator
├── run_pipeline.py                # Simple runner script
├── config.py                      # Configuration settings
├── bigquery_handler.py            # BigQuery operations module
├── data_processor.py              # Data processing module
├── model_trainer.py               # Model training & prediction module
├── test_modular_structure.py      # Module testing script
├── requirements.txt               # Python dependencies
├── README_Pipeline.md             # This documentation
├── models/
│   ├── __init__.py                # Python module init
│   ├── lstm_model.py              # LSTM model architecture
│   ├── lstm_forecaster.pth        # Pre-trained LSTM model
│   ├── product_scalers.pkl        # Product-specific scalers
│   └── ...
└── productpricetracker-123-*.json # BigQuery credentials
```

### Module Descriptions

**Core Pipeline:**

- `production_pipeline.py`: Main orchestrator that coordinates all components
- `run_pipeline.py`: Simple execution script for the complete pipeline

**Modular Components:**

- `bigquery_handler.py`: Handles all BigQuery operations (data fetching, uploads)
- `data_processor.py`: Data preprocessing, scaling, sequence creation
- `model_trainer.py`: Model loading, fine-tuning, and prediction generation
- `models/lstm_model.py`: LSTM architecture and dataset classes

**Configuration & Testing:**

- `config.py`: Centralized configuration for all pipeline settings
- `test_modular_structure.py`: Verification script for modular components

## Model Architecture

### LSTM Seq2Seq Architecture

```
Input Sequence (30 days) → Encoder LSTM → Hidden State
                                ↓
Hidden State → Decoder LSTM → 7 Predictions
```

**Model Specifications:**

- Input Size: 1 (price only)
- Hidden Size: 128 units
- Layers: 2 LSTM layers
- Output: 7 daily predictions
- Architecture: Encoder-Decoder with attention

### Training Process

1. **Data Preparation**: Create sliding windows from historical prices
2. **Per-Product Scaling**: Normalize each variant individually
3. **Sequence Creation**: Generate input-target pairs
4. **Fine-tuning**: Adapt pre-trained weights to new data
5. **Validation**: Monitor loss convergence

## Monitoring and Logging

### Log Levels

- **INFO**: Normal operation progress
- **WARNING**: Non-critical issues (missing variants)
- **ERROR**: Critical failures requiring attention

### Log Outputs

- Console output for real-time monitoring
- File output to `production_pipeline.log`

### Key Metrics Logged

- Number of variants processed
- Training loss progression
- Prediction accuracy statistics
- Execution time benchmarks

## Troubleshooting

### Common Issues

**"Model file not found"**

- Ensure `models/lstm_forecaster.pth` exists
- Check file permissions and path

**"BigQuery authentication failed"**

- Verify credentials JSON file exists
- Check service account permissions
- Ensure project ID is correct

**"No data returned from BigQuery"**

- Check table names in config
- Verify date ranges in query
- Ensure data exists for last 60 days

**"Fine-tuning failed"**

- Check GPU/CPU memory availability
- Reduce batch size if out of memory
- Verify input data format

### Performance Optimization

**Memory Usage**

- Reduce `BATCH_SIZE` for limited RAM
- Process variants in smaller chunks
- Use CPU if GPU memory insufficient

**Speed Optimization**

- Increase `BATCH_SIZE` for faster training
- Use GPU acceleration when available
- Parallel data loading with DataLoader

**Accuracy Improvement**

- Increase `FINE_TUNE_EPOCHS` for better adaptation
- Adjust `SEQUENCE_LENGTH` for more context
- Tune learning rate for specific datasets

## Production Deployment

### Scheduled Execution

```bash
# Daily execution at 6 AM
0 6 * * * cd /path/to/project && python run_pipeline.py
```

### Monitoring Setup

- Set up log monitoring alerts
- Monitor BigQuery table row counts
- Track prediction accuracy over time
- Set up failure notification system

### Backup Strategy

- Regular model checkpoint saves
- Historical prediction data retention
- Configuration version control
- Credential rotation schedule

## Support

For issues or questions:

1. Check the log files for detailed error information
2. Verify all prerequisites are met
3. Test BigQuery connection independently
4. Validate model file integrity

---

**Last Updated**: $(date)
**Version**: 1.0
**Compatibility**: Python 3.8+, PyTorch 1.9+, BigQuery API v3+
