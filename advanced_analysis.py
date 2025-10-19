"""
Advanced Analysis Module for Disability Rights Data
Text mining, NLP, and statistical analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter
import re
import json
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextAnalyzer:
    """Advanced text analysis for disability rights documents"""
    
    def __init__(self):
        # Key disability rights terms
        self.key_terms = {
            'legislation': ['ADA', 'IDEA', 'Section 504', 'Rehabilitation Act', 'Fair Housing'],
            'concepts': ['accessibility', 'accommodation', 'discrimination', 'inclusion', 'integration'],
            'movements': ['independent living', 'deinstitutionalization', 'mainstreaming', 'self-advocacy'],
            'groups': ['disability rights', 'disabled', 'handicapped', 'special needs']
        }
        
    def extract_legislative_references(self, text: str) -> List[str]:
        """Extract references to disability legislation"""
        
        patterns = [
            r'Americans with Disabilities Act|ADA',
            r'Section 504|Rehabilitation Act',
            r'IDEA|Individuals with Disabilities Education Act',
            r'Education for All Handicapped Children Act|EHA',
            r'Fair Housing Amendments Act',
            r'Air Carrier Access Act',
            r'P\.L\.\s+\d+-\d+',  # Public Law format
            r'\d+\s+U\.S\.C\.\s+§?\s*\d+',  # USC citations
        ]
        
        references = []
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            references.extend(matches)
        
        return list(set(references))
    
    def analyze_sentiment_keywords(self, text: str) -> Dict[str, int]:
        """Analyze sentiment using keyword matching"""
        
        positive_keywords = [
            'rights', 'access', 'inclusion', 'equality', 'opportunity',
            'independence', 'empowerment', 'accommodation', 'support'
        ]
        
        negative_keywords = [
            'discrimination', 'exclusion', 'barrier', 'segregation',
            'denial', 'violation', 'inaccessible', 'inadequate'
        ]
        
        text_lower = text.lower()
        
        sentiment = {
            'positive': sum(1 for word in positive_keywords if word in text_lower),
            'negative': sum(1 for word in negative_keywords if word in text_lower),
            'neutral': 0
        }
        
        total = sentiment['positive'] + sentiment['negative']
        if total > 0:
            sentiment['score'] = (sentiment['positive'] - sentiment['negative']) / total
        else:
            sentiment['score'] = 0.0
        
        return sentiment
    
    def extract_dates_and_periods(self, text: str) -> List[str]:
        """Extract dates and time periods from text"""
        
        date_patterns = [
            r'\b\d{4}\b',  # Years
            r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
            r'\d{1,2}/\d{1,2}/\d{2,4}',  # Date formats
        ]
        
        dates = []
        for pattern in date_patterns:
            matches = re.findall(pattern, text)
            dates.extend(matches)
        
        return dates
    
    def calculate_readability(self, text: str) -> Dict[str, float]:
        """Calculate basic readability metrics"""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Count sentences (approximate)
        sentences = re.split(r'[.!?]+', text)
        sentence_count = len([s for s in sentences if s.strip()])
        
        # Count words
        words = text.split()
        word_count = len(words)
        
        # Count syllables (approximate)
        def count_syllables(word):
            word = word.lower()
            count = len(re.findall(r'[aeiou]+', word))
            return max(1, count)
        
        syllable_count = sum(count_syllables(w) for w in words)
        
        if sentence_count == 0 or word_count == 0:
            return {'flesch_reading_ease': 0, 'avg_sentence_length': 0, 'avg_word_length': 0}
        
        # Flesch Reading Ease (approximate)
        avg_sentence_length = word_count / sentence_count
        avg_syllables_per_word = syllable_count / word_count
        flesch = 206.835 - 1.015 * avg_sentence_length - 84.6 * avg_syllables_per_word
        
        avg_word_length = sum(len(w) for w in words) / word_count
        
        return {
            'flesch_reading_ease': round(flesch, 2),
            'avg_sentence_length': round(avg_sentence_length, 2),
            'avg_word_length': round(avg_word_length, 2),
            'word_count': word_count,
            'sentence_count': sentence_count
        }
    
    def identify_stakeholders(self, text: str) -> List[str]:
        """Identify key stakeholders mentioned in text"""
        
        stakeholder_patterns = {
            'Government Agencies': [
                'Department of Education', 'DOE', 'HEW', 'HHS',
                'Department of Health and Human Services',
                'Department of Justice', 'DOJ',
                'Office for Civil Rights', 'OCR',
                'Equal Employment Opportunity Commission', 'EEOC'
            ],
            'Advocacy Organizations': [
                'ADAPT', 'National Council on Disability', 'NCD',
                'Disability Rights Education and Defense Fund', 'DREDF',
                'Center for Independent Living', 'CIL',
                'American Association of People with Disabilities', 'AAPD'
            ],
            'Political Figures': [
                'Senator Harkin', 'Senator Dole', 'Representative Coelho',
                'President Bush', 'President Reagan', 'President Carter'
            ]
        }
        
        found_stakeholders = []
        text_lower = text.lower()
        
        for category, entities in stakeholder_patterns.items():
            for entity in entities:
                if entity.lower() in text_lower:
                    found_stakeholders.append((category, entity))
        
        return found_stakeholders


class StatisticalAnalyzer:
    """Statistical analysis of disability data trends"""
    
    def __init__(self):
        pass
    
    def analyze_temporal_trends(self, df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
        """Analyze trends over time"""
        
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df['year'] = df[date_col].dt.year
        df['month'] = df[date_col].dt.month
        df['quarter'] = df[date_col].dt.quarter
        
        # Aggregate by year
        yearly = df.groupby('year').size().reset_index(name='count')
        
        # Calculate year-over-year growth
        yearly['yoy_growth'] = yearly['count'].pct_change() * 100
        
        # Calculate moving average
        yearly['moving_avg_3yr'] = yearly['count'].rolling(window=3, center=True).mean()
        
        return yearly
    
    def identify_peak_periods(self, df: pd.DataFrame, date_col: str = 'date', n: int = 5) -> pd.DataFrame:
        """Identify peak periods of activity"""
        
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df['year_month'] = df[date_col].dt.to_period('M')
        
        monthly_counts = df.groupby('year_month').size().reset_index(name='count')
        monthly_counts = monthly_counts.sort_values('count', ascending=False).head(n)
        
        return monthly_counts
    
    def calculate_correlation_matrix(self, df: pd.DataFrame, numeric_cols: List[str]) -> pd.DataFrame:
        """Calculate correlation between numeric features"""
        
        correlation_matrix = df[numeric_cols].corr()
        return correlation_matrix
    
    def detect_outliers(self, series: pd.Series) -> pd.DataFrame:
        """Detect outliers using IQR method"""
        
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = series[(series < lower_bound) | (series > upper_bound)]
        
        return pd.DataFrame({
            'value': outliers.values,
            'index': outliers.index,
            'type': ['high' if v > upper_bound else 'low' for v in outliers.values]
        })
    
    def generate_summary_statistics(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive summary statistics"""
        
        summary = {
            'total_records': len(df),
            'columns': list(df.columns),
            'numeric_summary': {},
            'categorical_summary': {}
        }
        
        # Numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            summary['numeric_summary'][col] = {
                'mean': float(df[col].mean()),
                'median': float(df[col].median()),
                'std': float(df[col].std()),
                'min': float(df[col].min()),
                'max': float(df[col].max())
            }
        
        # Categorical columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            value_counts = df[col].value_counts()
            summary['categorical_summary'][col] = {
                'unique_values': int(df[col].nunique()),
                'top_values': value_counts.head(5).to_dict()
            }
        
        return summary


class DataQualityChecker:
    """Check data quality and completeness"""
    
    def __init__(self):
        pass
    
    def check_completeness(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check data completeness"""
        
        total_rows = len(df)
        
        completeness = pd.DataFrame({
            'column': df.columns,
            'missing_count': [df[col].isnull().sum() for col in df.columns],
            'missing_percent': [(df[col].isnull().sum() / total_rows * 100) for col in df.columns],
            'filled_count': [df[col].notna().sum() for col in df.columns],
            'filled_percent': [(df[col].notna().sum() / total_rows * 100) for col in df.columns]
        })
        
        return completeness.sort_values('missing_percent', ascending=False)
    
    def check_duplicates(self, df: pd.DataFrame) -> Dict:
        """Check for duplicate records"""
        
        total_duplicates = df.duplicated().sum()
        duplicate_percent = (total_duplicates / len(df) * 100) if len(df) > 0 else 0
        
        return {
            'total_duplicates': int(total_duplicates),
            'duplicate_percent': round(duplicate_percent, 2),
            'unique_records': int(len(df.drop_duplicates()))
        }
    
    def validate_date_ranges(self, df: pd.DataFrame, date_col: str, 
                           expected_start: str = "1975-01-01",
                           expected_end: str = "2000-12-31") -> Dict:
        """Validate date ranges"""
        
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        actual_start = df[date_col].min()
        actual_end = df[date_col].max()
        
        expected_start = pd.to_datetime(expected_start)
        expected_end = pd.to_datetime(expected_end)
        
        out_of_range = df[
            (df[date_col] < expected_start) | 
            (df[date_col] > expected_end)
        ]
        
        return {
            'actual_start': str(actual_start),
            'actual_end': str(actual_end),
            'expected_start': str(expected_start),
            'expected_end': str(expected_end),
            'records_out_of_range': len(out_of_range),
            'valid_records': len(df) - len(out_of_range)
        }


def analyze_disability_corpus(data_dir: str = "disability_data"):
    """Main analysis function"""
    
    print("=" * 70)
    print("Advanced Disability Rights Data Analysis")
    print("=" * 70)
    print()
    
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"❌ Data directory not found: {data_dir}")
        print("   Run disability_data_retrieval.py first")
        return
    
    # Initialize analyzers
    text_analyzer = TextAnalyzer()
    stat_analyzer = StatisticalAnalyzer()
    quality_checker = DataQualityChecker()
    
    # Load timeline data
    timeline_file = data_path / "disability_timeline.csv"
    if timeline_file.exists():
        print("📊 Analyzing timeline data...")
        timeline_df = pd.read_csv(timeline_file)
        
        # Temporal analysis
        trends = stat_analyzer.analyze_temporal_trends(timeline_df)
        print(f"   ✓ Analyzed {len(timeline_df)} timeline events")
        
        # Peak periods
        peaks = stat_analyzer.identify_peak_periods(timeline_df)
        print(f"   ✓ Identified {len(peaks)} peak activity periods")
        
        # Save analysis
        trends.to_csv(data_path / "timeline_trends.csv", index=False)
        peaks.to_csv(data_path / "peak_periods.csv", index=False)
        print()
    
    # Analyze GovInfo data
    govinfo_file = data_path / "govinfo_federal_register.csv"
    if govinfo_file.exists():
        print("📄 Analyzing Federal Register documents...")
        govinfo_df = pd.read_csv(govinfo_file)
        
        # Quality check
        completeness = quality_checker.check_completeness(govinfo_df)
        print(f"   Data completeness:")
        print(completeness.head())
        
        # Summary stats
        summary = stat_analyzer.generate_summary_statistics(govinfo_df)
        print(f"   ✓ Generated summary statistics")
        
        # Save analysis
        completeness.to_csv(data_path / "govinfo_quality_report.csv", index=False)
        
        with open(data_path / "govinfo_summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        print()
    
    # Generate comprehensive report
    print("📋 Generating comprehensive analysis report...")
    
    report = {
        'analysis_date': pd.Timestamp.now().isoformat(),
        'data_directory': str(data_path),
        'files_analyzed': list(data_path.glob("*.csv")),
        'key_findings': {
            'total_timeline_events': len(timeline_df) if 'timeline_df' in locals() else 0,
            'date_range': f"{timeline_df['date'].min()} to {timeline_df['date'].max()}" if 'timeline_df' in locals() else 'N/A'
        }
    }
    
    with open(data_path / "analysis_report.json", 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print("✅ Advanced analysis complete!")
    print(f"📁 Reports saved to: {data_path}")
    print()
    print("Generated files:")
    print("  - timeline_trends.csv: Temporal trend analysis")
    print("  - peak_periods.csv: Peak activity periods")
    print("  - govinfo_quality_report.csv: Data quality metrics")
    print("  - analysis_report.json: Comprehensive analysis summary")


if __name__ == "__main__":
    analyze_disability_corpus()
