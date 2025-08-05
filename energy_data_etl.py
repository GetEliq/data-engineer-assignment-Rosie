"""
Eliq Energy Data ETL Script
==========================

This script transforms energy consumption data from array format (24 hourly values per day)
into individual hourly records optimized for analytical queries.

Author: Rosie Arntsen
Date: July 31, 2025
Assignment: Eliq Lead Data Scientist Technical Task
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Any
import argparse
import logging

# Optional ML imports for advanced imputation (install if needed)
try:
    from sklearn.impute import KNNImputer, IterativeImputer
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("Warning: scikit-learn not available. ML-based imputation methods will be disabled.")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnergyDataETL:
    """
    ETL processor for energy consumption data transformation.
    
    Transforms data from:
    - Array format: One row per day with 24-element energy consumption array
    
    To:
    - Hourly format: One row per hour with individual energy consumption values
    """
    
    def __init__(self, timezone: str = 'Europe/Vilnius'):
        """
        Initialize ETL processor.
        
        Args:
            timezone: Timezone for date/time processing (default: Europe/Vilnius)
        """
        self.timezone = pytz.timezone(timezone)
        self.utc = pytz.UTC
        
    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Load energy data from parquet file.
        
        Args:
            file_path: Path to the parquet file
            
        Returns:
            DataFrame with raw energy data
        """
        logger.info(f"Loading data from {file_path}")
        
        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Successfully loaded {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def validate_input_data(self, df: pd.DataFrame) -> None:
        """
        Validate input data quality and structure.
        
        Args:
            df: Input DataFrame to validate
            
        Raises:
            ValueError: If data validation fails
        """
        logger.info("Validating input data...")
        
        # Check required columns
        required_columns = ['client_id', 'date', 'ext_dev_ref', 'energy_consumption', 'resolution']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.any():
            logger.warning(f"Found null values: {null_counts[null_counts > 0].to_dict()}")
        
        # Validate energy consumption arrays
        array_lengths = [len(arr) if isinstance(arr, (list, np.ndarray)) else 0 
                        for arr in df['energy_consumption']]
        
        if not all(length == 24 for length in array_lengths):
            invalid_lengths = set(array_lengths) - {24}
            raise ValueError(f"All energy_consumption arrays must have 24 values. Found lengths: {invalid_lengths}")
        
        logger.info("Input data validation passed")
    
    def impute_missing(self, df: pd.DataFrame, method: str = 'mode') -> pd.DataFrame:
        """
        Impute missing values in energy_consumption arrays using various strategies.
        
        Args:
            df: Input DataFrame with energy_consumption arrays
            method: Imputation method options:
                Basic: 'mode', 'mean', 'median', 'interpolate', 'forward_fill', 'backward_fill'
                ML-based: 'knn', 'iterative', 'random_forest', 'linear_regression'
            
        Returns:
            DataFrame with imputed values
        """
        logger.info(f"Imputing missing energy values with {method}")
        imputed_df = df.copy()
        
        # Basic statistical methods
        if method == 'mode':
            # Use global mode across all energy values
            all_vals = np.concatenate(df['energy_consumption'].tolist()).astype(float)
            fill_val = pd.Series(all_vals).mode()[0]
            imputed_df['energy_consumption'] = imputed_df['energy_consumption'].apply(
                lambda arr: [fill_val if pd.isna(x) else x for x in arr]
            )
            
        elif method == 'mean':
            # Use global mean across all energy values
            all_vals = np.concatenate(df['energy_consumption'].tolist()).astype(float)
            fill_val = np.nanmean(all_vals)
            imputed_df['energy_consumption'] = imputed_df['energy_consumption'].apply(
                lambda arr: [fill_val if pd.isna(x) else x for x in arr]
            )
            
        elif method == 'median':
            # Use global median across all energy values
            all_vals = np.concatenate(df['energy_consumption'].tolist()).astype(float)
            fill_val = np.nanmedian(all_vals)
            imputed_df['energy_consumption'] = imputed_df['energy_consumption'].apply(
                lambda arr: [fill_val if pd.isna(x) else x for x in arr]
            )
            
        elif method == 'interpolate':
            # Linear interpolation within each day's array
            def interpolate_array(arr):
                arr_float = np.array(arr, dtype=float)
                if pd.isna(arr_float).any():
                    # Create index for interpolation
                    valid_mask = ~pd.isna(arr_float)
                    if valid_mask.sum() >= 2:  # Need at least 2 points for interpolation
                        indices = np.arange(len(arr_float))
                        arr_float[~valid_mask] = np.interp(indices[~valid_mask], 
                                                         indices[valid_mask], 
                                                         arr_float[valid_mask])
                    else:
                        # Fallback to mean if insufficient points
                        arr_float[pd.isna(arr_float)] = np.nanmean(arr_float)
                return arr_float.tolist()
            
            imputed_df['energy_consumption'] = imputed_df['energy_consumption'].apply(interpolate_array)
            
        elif method == 'forward_fill':
            # Forward fill within each day's array
            def forward_fill_array(arr):
                arr_float = np.array(arr, dtype=float)
                mask = pd.isna(arr_float)
                if mask.any():
                    # Forward fill
                    for i in range(1, len(arr_float)):
                        if pd.isna(arr_float[i]) and not pd.isna(arr_float[i-1]):
                            arr_float[i] = arr_float[i-1]
                    # If first value is NaN, backward fill
                    for i in range(len(arr_float)-2, -1, -1):
                        if pd.isna(arr_float[i]) and not pd.isna(arr_float[i+1]):
                            arr_float[i] = arr_float[i+1]
                return arr_float.tolist()
            
            imputed_df['energy_consumption'] = imputed_df['energy_consumption'].apply(forward_fill_array)
            
        elif method == 'backward_fill':
            # Backward fill within each day's array
            def backward_fill_array(arr):
                arr_float = np.array(arr, dtype=float)
                mask = pd.isna(arr_float)
                if mask.any():
                    # Backward fill
                    for i in range(len(arr_float)-2, -1, -1):
                        if pd.isna(arr_float[i]) and not pd.isna(arr_float[i+1]):
                            arr_float[i] = arr_float[i+1]
                    # If last value is NaN, forward fill
                    for i in range(1, len(arr_float)):
                        if pd.isna(arr_float[i]) and not pd.isna(arr_float[i-1]):
                            arr_float[i] = arr_float[i-1]
                return arr_float.tolist()
            
            imputed_df['energy_consumption'] = imputed_df['energy_consumption'].apply(backward_fill_array)
        
        # ML-based imputation methods
        elif method in ['knn', 'iterative', 'random_forest', 'linear_regression']:
            if not SKLEARN_AVAILABLE:
                logger.warning(f"scikit-learn not available. Falling back to mode imputation.")
                return self.impute_missing(df, method='mode')
            
            imputed_df = self._ml_based_imputation(df, method)
            
        else:
            available_methods = ['mode', 'mean', 'median', 'interpolate', 'forward_fill', 'backward_fill']
            if SKLEARN_AVAILABLE:
                available_methods.extend(['knn', 'iterative', 'random_forest', 'linear_regression'])
            raise ValueError(f"Unsupported imputation method: {method}. "
                           f"Choose from: {', '.join(available_methods)}")
        
        # Count imputed values for logging
        all_vals_original = np.concatenate(df['energy_consumption'].tolist())
        total_missing = pd.isna(all_vals_original).sum()
        logger.info(f"Imputed {total_missing} missing values using {method}")
        
        return imputed_df
    
    def _ml_based_imputation(self, df: pd.DataFrame, method: str) -> pd.DataFrame:
        """
        Perform ML-based imputation on energy consumption arrays.
        
        Args:
            df: Input DataFrame with energy_consumption arrays
            method: ML method ('knn', 'iterative', 'random_forest', 'linear_regression')
            
        Returns:
            DataFrame with ML-imputed values
        """
        logger.info(f"Performing {method} imputation...")
        
        # Convert arrays to matrix format for ML processing
        energy_matrix = np.array([arr for arr in df['energy_consumption']])
        
        # Check if there are missing values to impute
        if not pd.isna(energy_matrix).any():
            logger.info("No missing values found, returning original data")
            return df.copy()
        
        try:
            if method == 'knn':
                # KNN Imputation - uses similarity of consumption patterns
                imputer = KNNImputer(n_neighbors=5, weights='distance')
                imputed_matrix = imputer.fit_transform(energy_matrix)
                logger.info("KNN imputation completed using 5 nearest neighbors")
                
            elif method == 'iterative':
                # Iterative imputation - models each feature as function of others
                imputer = IterativeImputer(
                    estimator=RandomForestRegressor(n_estimators=10, random_state=42),
                    max_iter=10,
                    random_state=42
                )
                imputed_matrix = imputer.fit_transform(energy_matrix)
                logger.info("Iterative imputation completed using Random Forest estimator")
                
            elif method == 'random_forest':
                # Custom Random Forest imputation for energy patterns
                imputed_matrix = self._random_forest_imputation(energy_matrix)
                logger.info("Random Forest imputation completed")
                
            elif method == 'linear_regression':
                # Linear regression based on hourly patterns
                imputed_matrix = self._linear_regression_imputation(energy_matrix)
                logger.info("Linear regression imputation completed")
                
            else:
                raise ValueError(f"Unknown ML method: {method}")
            
            # Convert back to DataFrame format
            imputed_df = df.copy()
            imputed_df['energy_consumption'] = [row.tolist() for row in imputed_matrix]
            
            return imputed_df
            
        except Exception as e:
            logger.warning(f"ML imputation failed ({e}), falling back to mode imputation")
            return self.impute_missing(df, method='mode')
    
    def _random_forest_imputation(self, energy_matrix: np.ndarray) -> np.ndarray:
        """Custom Random Forest imputation considering energy consumption patterns."""
        imputed_matrix = energy_matrix.copy()
        
        for hour in range(24):
            # Find rows where this hour has missing values
            missing_mask = pd.isna(energy_matrix[:, hour])
            
            if missing_mask.any():
                # Use other hours as features to predict this hour
                feature_hours = [h for h in range(24) if h != hour]
                
                # Get training data (rows without missing values for this hour)
                train_mask = ~missing_mask
                if train_mask.sum() > 5:  # Need minimum training samples
                    
                    X_train = energy_matrix[train_mask][:, feature_hours]
                    y_train = energy_matrix[train_mask, hour]
                    
                    # Remove rows with any missing features
                    complete_rows = ~pd.isna(X_train).any(axis=1)
                    if complete_rows.sum() > 5:
                        X_train = X_train[complete_rows]
                        y_train = y_train[complete_rows]
                        
                        # Train Random Forest
                        rf = RandomForestRegressor(n_estimators=10, random_state=42)
                        rf.fit(X_train, y_train)
                        
                        # Predict missing values
                        X_missing = energy_matrix[missing_mask][:, feature_hours]
                        # Only predict for rows with complete features
                        complete_missing = ~pd.isna(X_missing).any(axis=1)
                        
                        if complete_missing.any():
                            predictions = rf.predict(X_missing[complete_missing])
                            missing_indices = np.where(missing_mask)[0][complete_missing]
                            imputed_matrix[missing_indices, hour] = predictions
                
                # Fill any remaining missing values with hour-specific mean
                still_missing = pd.isna(imputed_matrix[:, hour])
                if still_missing.any():
                    hour_mean = np.nanmean(energy_matrix[:, hour])
                    imputed_matrix[still_missing, hour] = hour_mean
        
        return imputed_matrix
    
    def _linear_regression_imputation(self, energy_matrix: np.ndarray) -> np.ndarray:
        """Linear regression imputation based on temporal relationships."""
        imputed_matrix = energy_matrix.copy()
        
        for hour in range(24):
            missing_mask = pd.isna(energy_matrix[:, hour])
            
            if missing_mask.any():
                # Use adjacent hours as predictors (stronger temporal correlation)
                feature_hours = []
                if hour > 0:
                    feature_hours.append(hour - 1)
                if hour < 23:
                    feature_hours.append(hour + 1)
                
                if len(feature_hours) > 0:
                    train_mask = ~missing_mask
                    
                    if train_mask.sum() > 3:  # Minimum samples for linear regression
                        X_train = energy_matrix[train_mask][:, feature_hours]
                        y_train = energy_matrix[train_mask, hour]
                        
                        # Remove rows with missing features
                        complete_rows = ~pd.isna(X_train).any(axis=1)
                        if complete_rows.sum() > 3:
                            X_train = X_train[complete_rows]
                            y_train = y_train[complete_rows]
                            
                            # Train linear regression
                            lr = LinearRegression()
                            lr.fit(X_train, y_train)
                            
                            # Predict missing values
                            X_missing = energy_matrix[missing_mask][:, feature_hours]
                            complete_missing = ~pd.isna(X_missing).any(axis=1)
                            
                            if complete_missing.any():
                                predictions = lr.predict(X_missing[complete_missing])
                                # Ensure predictions are non-negative (energy consumption)
                                predictions = np.maximum(predictions, 0)
                                missing_indices = np.where(missing_mask)[0][complete_missing]
                                imputed_matrix[missing_indices, hour] = predictions
                
                # Fill remaining with interpolation or mean
                still_missing = pd.isna(imputed_matrix[:, hour])
                if still_missing.any():
                    hour_mean = np.nanmean(energy_matrix[:, hour])
                    imputed_matrix[still_missing, hour] = hour_mean
        
        return imputed_matrix
    
    def explode_energy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform array-based energy data into hourly records.
        
        Args:
            df: Input DataFrame with energy_consumption arrays
            
        Returns:
            DataFrame with individual hourly records
        """
        logger.info("Exploding energy consumption arrays into hourly records...")
        
        # Create list to store transformed records
        hourly_records = []
        
        for _, row in df.iterrows():
            # Parse date string to datetime
            date_str = row['date']
            if isinstance(date_str, str):
                base_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            else:
                base_date = date_str
            
            # Create hourly records for each energy value
            energy_values = row['energy_consumption']
            
            for hour in range(24):
                # Create timezone-aware datetime
                hour_datetime = datetime.combine(base_date, datetime.min.time().replace(hour=hour))
                local_datetime = self.timezone.localize(hour_datetime)
                utc_datetime = local_datetime.astimezone(self.utc)
                
                hourly_record = {
                    'client_id': row['client_id'],
                    'meter_id': row['ext_dev_ref'],
                    'date': base_date,
                    'hour_of_day': hour,
                    'timestamp_local': local_datetime,
                    'timestamp_utc': utc_datetime,
                    'energy_consumption': float(energy_values[hour]),
                    'resolution': row['resolution'],
                    # Add derived analytical columns
                    'day_of_week': base_date.weekday(),  # 0=Monday, 6=Sunday
                    'is_weekend': base_date.weekday() >= 5,
                    'month': base_date.month,
                    'quarter': (base_date.month - 1) // 3 + 1,
                    'year': base_date.year,
                    'is_business_hour': 8 <= hour <= 17,  # 8 AM to 6 PM
                    'time_of_day_category': self._categorize_time_of_day(hour)
                }
                
                hourly_records.append(hourly_record)
        
        result_df = pd.DataFrame(hourly_records)
        logger.info(f"Created {len(result_df)} hourly records from {len(df)} daily records")
        
        return result_df
    
    def _categorize_time_of_day(self, hour: int) -> str:
        """
        Categorize hour into time periods for energy analysis.
        
        Args:
            hour: Hour of day (0-23)
            
        Returns:
            Time category string
        """
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 18:
            return 'afternoon'
        elif 18 <= hour < 22:
            return 'evening'
        else:
            return 'night'
    
    def add_analytical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add additional features useful for energy analytics.
        
        Args:
            df: DataFrame with hourly energy records
            
        Returns:
            DataFrame with additional analytical features
        """
        logger.info("Adding analytical features...")
        
        # Sort by timestamp for proper window calculations
        df = df.sort_values(['client_id', 'meter_id', 'timestamp_local']).reset_index(drop=True)
        
        # Calculate rolling averages (useful for trend analysis)
        df['energy_consumption_24h_avg'] = df.groupby(['client_id', 'meter_id'])['energy_consumption'].rolling(
            window=24, min_periods=1).mean().values
        
        df['energy_consumption_7d_avg'] = df.groupby(['client_id', 'meter_id'])['energy_consumption'].rolling(
            window=24*7, min_periods=1).mean().values
        
        # Calculate daily aggregates
        daily_stats = df.groupby(['client_id', 'meter_id', 'date'])['energy_consumption'].agg([
            'sum', 'mean', 'min', 'max', 'std'
        ]).reset_index()
        daily_stats.columns = ['client_id', 'meter_id', 'date', 'daily_total', 'daily_mean', 
                              'daily_min', 'daily_max', 'daily_std']
        
        # Merge daily stats back to hourly data
        df = df.merge(daily_stats, on=['client_id', 'meter_id', 'date'], how='left')
        
        # Calculate hour-of-day statistics (useful for pattern analysis)
        hourly_patterns = df.groupby(['client_id', 'meter_id', 'hour_of_day'])['energy_consumption'].agg([
            'mean', 'std'
        ]).reset_index()
        hourly_patterns.columns = ['client_id', 'meter_id', 'hour_of_day', 'hourly_pattern_mean', 'hourly_pattern_std']
        
        df = df.merge(hourly_patterns, on=['client_id', 'meter_id', 'hour_of_day'], how='left')
        
        # Calculate energy consumption percentile rank within the day
        df['daily_consumption_rank'] = df.groupby(['client_id', 'meter_id', 'date'])['energy_consumption'].rank(pct=True)
        
        logger.info("Analytical features added successfully")
        return df
    
    def generate_data_quality_report(self, original_df: pd.DataFrame, transformed_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate data quality and transformation report.
        
        Args:
            original_df: Original input DataFrame
            transformed_df: Transformed output DataFrame
            
        Returns:
            Dictionary with quality metrics
        """
        report = {
            'input_records': len(original_df),
            'output_records': len(transformed_df),
            'expansion_ratio': len(transformed_df) / len(original_df),
            'date_range': {
                'start': transformed_df['date'].min(),
                'end': transformed_df['date'].max(),
                'days': (transformed_df['date'].max() - transformed_df['date'].min()).days + 1
            },
            'unique_clients': transformed_df['client_id'].nunique(),
            'unique_meters': transformed_df['meter_id'].nunique(),
            'energy_consumption_stats': {
                'min': transformed_df['energy_consumption'].min(),
                'max': transformed_df['energy_consumption'].max(),
                'mean': transformed_df['energy_consumption'].mean(),
                'std': transformed_df['energy_consumption'].std()
            },
            'completeness': {
                'total_expected_records': len(original_df) * 24,  # 24 hours per day
                'actual_records': len(transformed_df),
                'completeness_rate': len(transformed_df) / (len(original_df) * 24)
            }
        }
        
        return report
    
    def save_transformed_data(self, df: pd.DataFrame, output_path: str, format: str = 'parquet') -> None:
        """
        Save transformed data to file.
        
        Args:
            df: Transformed DataFrame
            output_path: Output file path
            format: Output format ('parquet', 'csv', or 'json')
        """
        logger.info(f"Saving transformed data to {output_path} in {format} format")
        
        try:
            if format.lower() == 'parquet':
                df.to_parquet(output_path, index=False)
            elif format.lower() == 'csv':
                df.to_csv(output_path, index=False)
            elif format.lower() == 'json':
                df.to_json(output_path, orient='records', date_format='iso')
            else:
                raise ValueError(f"Unsupported format: {format}")
                
            logger.info(f"Successfully saved {len(df)} records")
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            raise
    
    def run_etl_pipeline(self, input_path: str, output_path: str, output_format: str = 'parquet') -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            input_path: Path to input parquet file
            output_path: Path for output file
            output_format: Format for output file
            
        Returns:
            Data quality report
        """
        logger.info("Starting ETL pipeline...")
        
        # Load and validate data
        original_df = self.load_data(input_path)
        self.validate_input_data(original_df)
        # Impute missing values
        imputed_df = self.impute_missing(original_df)
        
        # Transform data using imputed dataset
        transformed_df = self.explode_energy_data(imputed_df)
        transformed_df = self.add_analytical_features(transformed_df)
        
        # Generate quality report
        quality_report = self.generate_data_quality_report(original_df, transformed_df)
        
        # Save results
        self.save_transformed_data(transformed_df, output_path, output_format)
        
        logger.info("ETL pipeline completed successfully")
        return quality_report


def main():
    """Main execution function with command line interface."""
    parser = argparse.ArgumentParser(description='Eliq Energy Data ETL Processor')
    parser.add_argument('--input', '-i', required=True, help='Input parquet file path')
    parser.add_argument('--output', '-o', required=True, help='Output file path')
    parser.add_argument('--format', '-f', default='parquet', choices=['parquet', 'csv', 'json'],
                       help='Output format (default: parquet)')
    parser.add_argument('--timezone', '-tz', default='Europe/Vilnius',
                       help='Timezone for datetime processing (default: Europe/Vilnius)')
    
    args = parser.parse_args()
    
    # Initialize ETL processor
    etl = EnergyDataETL(timezone=args.timezone)
    
    # Run pipeline
    try:
        quality_report = etl.run_etl_pipeline(args.input, args.output, args.format)
        
        # Print quality report
        print("\n" + "="*50)
        print("DATA TRANSFORMATION QUALITY REPORT")
        print("="*50)
        print(f"Input records: {quality_report['input_records']:,}")
        print(f"Output records: {quality_report['output_records']:,}")
        print(f"Expansion ratio: {quality_report['expansion_ratio']:.1f}x")
        print(f"Date range: {quality_report['date_range']['start']} to {quality_report['date_range']['end']}")
        print(f"Total days: {quality_report['date_range']['days']}")
        print(f"Unique clients: {quality_report['unique_clients']}")
        print(f"Unique meters: {quality_report['unique_meters']}")
        print(f"Data completeness: {quality_report['completeness']['completeness_rate']:.1%}")
        print("\nEnergy consumption statistics:")
        stats = quality_report['energy_consumption_stats']
        print(f"  Min: {stats['min']:.2f}")
        print(f"  Max: {stats['max']:.2f}")
        print(f"  Mean: {stats['mean']:.2f}")
        print(f"  Std Dev: {stats['std']:.2f}")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
