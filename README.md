# Assignment Stage - Technical Task

This folder contains all materials related to the technical assignment for Eliq's Lead Data Scientist position.

## Assignment Overview
- **Task**: Design and implement energy data ETL pipeline
- **Time Limit**: 3-4 hours
- **Deadline**: August 6, 2025
- **Data**: Daily energy consumption arrays → hourly analytical records

## Submission Files

### Core Deliverables
- `part1_data_platform_architecture.md` - Data platform architecture design (Part 1)
- `part2_etl_implementation.md` - ETL implementation documentation (Part 2)
- `energy_data_etl.py` - Production-ready ETL implementation
- `energy_data_analysis.ipynb` - Comprehensive analysis notebook with ML models
- `transformed_energy_data.parquet` - Final processed dataset (8,760 hourly records)

### Environment Setup
- `requirements.txt` - Python package dependencies (pip)
- `environment.yml` - Conda environment specification

### Data
- `data/home_assignment_raw_data.parquet` - Original dataset provided by Eliq

### Analysis Results
- Multiple ML models: forecasting, classification, anomaly detection
- 25+ analytical features for energy insights
- Data quality reports and visualizations
- Business value alignment with Eliq's platform

## Key Accomplishments
✅ **Data Transformation**: 365 daily records → 8,760 hourly records (24x expansion)  
✅ **Timezone Handling**: Proper Europe/Vilnius localization with UTC conversion  
✅ **Missing Value Treatment**: Mode-based imputation for data completeness  
✅ **Feature Engineering**: 25+ analytical columns for energy insights  
✅ **ML Models**: Forecasting, peak classification, and anomaly detection  
✅ **Production Quality**: Reusable ETL class with comprehensive validation  
✅ **Business Alignment**: Direct mapping to Eliq's Insights API capabilities  

## How to Run

### Option 1: Using pip (requirements.txt)
```bash
# Install dependencies
pip install -r requirements.txt

# Run ETL transformation
python energy_data_etl.py --input data/home_assignment_raw_data.parquet --output transformed_energy_data.parquet

# Or explore the Jupyter notebook
jupyter notebook energy_data_analysis.ipynb
```

### Option 2: Using conda (environment.yml)
```bash
# Create conda environment
conda env create -f environment.yml

# Activate environment
conda activate eliq-energy-etl

# Run ETL transformation
python energy_data_etl.py --input data/home_assignment_raw_data.parquet --output transformed_energy_data.parquet

# Or explore the Jupyter notebook
jupyter notebook energy_data_analysis.ipynb
```

## Next Steps
- Technical interview discussing implementation details
- Architecture review and scalability considerations
- Integration with Eliq's existing data infrastructure
