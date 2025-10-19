# Disability Rights Data Retrieval & Analysis Toolkit (1975-2000)

Automated toolkit for retrieving, analyzing, and visualizing historical US disability rights policy data from 1975-2000.

## Overview

This toolkit automates the collection and analysis of disability rights data from multiple sources:

- **GovInfo API**: Federal Register, Congressional Record, and government documents
- **National Archives Catalog**: Historical federal agency records
- **Census Bureau**: Disability statistics and demographic data
- **News Archives**: Historical newspaper coverage (framework provided)

## Features

✅ **Automated Data Retrieval**
- Search GovInfo for disability-related Federal Register notices
- Query National Archives Catalog for historical records
- Scrape Census Bureau disability statistics
- Built-in timeline of major disability rights milestones

✅ **Advanced Analysis**
- Text mining and keyword extraction
- Temporal trend analysis
- Statistical summaries and correlations
- Data quality checks

✅ **Comprehensive Visualizations**
- Timeline charts showing policy evolution
- Yearly trend analysis
- Event type distributions
- Decade-by-decade comparisons
- Interactive HTML dashboards

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup

1. **Clone or download the toolkit files**

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Get API keys (optional but recommended)**
   - GovInfo API key: Register at https://api.data.gov/signup/
   - Replace `'DEMO'` in the code with your actual API key

## Usage

### Step 1: Retrieve Data

Run the main retrieval script:

```bash
python disability_data_retrieval.py
```

This will:
- Create a `disability_data/` directory
- Generate a timeline of disability rights milestones
- Search GovInfo Federal Register for relevant documents
- Query National Archives Catalog
- Identify Census Bureau disability tables
- Save results in CSV, JSON, and Excel formats

**Output files:**
- `disability_timeline.csv/json/xlsx` - Key milestones (1975-2000)
- `govinfo_federal_register.csv/json/xlsx` - Federal Register documents
- `national_archives_records.csv/json/xlsx` - Archive catalog results
- `census_disability_tables.csv/json/xlsx` - Census data sources

### Step 2: Advanced Analysis

Run the analysis module:

```bash
python advanced_analysis.py
```

This performs:
- Temporal trend analysis
- Peak period identification
- Data quality checks
- Statistical summaries
- Text analysis on document corpus

**Output files:**
- `timeline_trends.csv` - Yearly trends with growth rates
- `peak_periods.csv` - Periods of highest activity
- `govinfo_quality_report.csv` - Data completeness metrics
- `analysis_report.json` - Comprehensive summary

### Step 3: Create Visualizations

Generate charts and dashboards:

```bash
python visualizations.py
```

This creates:
- Timeline visualization
- Yearly trends charts
- Event type distributions
- Decade comparisons
- Summary dashboard
- Interactive HTML timeline

**Output files** (in `visualizations/` directory):
- `timeline.png` - Visual timeline of events
- `yearly_trends.png` - Trend analysis charts
- `event_types.png` - Distribution of event types
- `decade_comparison.png` - Decade-by-decade analysis
- `dashboard.png` - Comprehensive overview
- `interactive_timeline.html` - Interactive web view

## Project Structure

```
.
├── disability_data_retrieval.py   # Main data retrieval script
├── advanced_analysis.py            # Statistical analysis module
├── visualizations.py               # Visualization generation
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── disability_data/                # Output directory (created automatically)
│   ├── *.csv                       # CSV data files
│   ├── *.json                      # JSON data files
│   └── *.xlsx                      # Excel data files
└── visualizations/                 # Visualization outputs (created automatically)
    ├── *.png                       # Chart images
    └── *.html                      # Interactive visualizations
```

## Customization

### Modify Search Queries

Edit the `queries` list in `disability_data_retrieval.py`:

```python
queries = [
    "Americans with Disabilities Act",
    "Section 504",
    "disability rights",
    "special education",
    # Add your custom queries here
]
```

### Adjust Date Ranges

Change the date parameters in search functions:

```python
results = retriever.search_govinfo(
    query="disability rights",
    start_date="1975-01-01",  # Modify as needed
    end_date="2000-12-31"     # Modify as needed
)
```

### Add Custom Milestones

Edit the `milestones` list in the `compile_disability_timeline()` method:

```python
milestones = [
    {
        'date': 'YYYY-MM-DD',
        'event': 'Event description',
        'type': 'legislation|regulation|activism|court_case',
        'impact': 'Impact description'
    },
    # Add more events...
]
```

### Customize Visualizations

Modify color schemes in `visualizations.py`:

```python
type_colors = {
    'legislation': '#2E86AB',  # Blue
    'regulation': '#A23B72',   # Purple
    'activism': '#F18F01',     # Orange
    'court_case': '#C73E1D'    # Red
}
```

## API Rate Limits

**GovInfo API:**
- Demo key: Limited requests per hour
- Registered key: Higher limits (recommended)
- The toolkit includes automatic rate limiting (1 second delay between requests)

**National Archives API:**
- No authentication required
- Be respectful with request frequency

**Census Bureau:**
- Web scraping for table discovery (no API key needed)
- Some tables require manual download

## Data Sources Coverage

### Pre-Internet Era (1975-2000)
The toolkit provides **automated access** to:
- Federal Register archives (1936-present)
- Congressional Record (1873-present)
- National Archives catalog descriptions
- Census Bureau metadata

For **full-text historical documents**, you may need:
- Institutional access to HeinOnline
- ProQuest Historical Newspapers subscription
- Physical visits to Federal Depository Libraries

### Free vs. Paid Access

**Free (via this toolkit):**
- GovInfo Federal Register (1994+ full text, earlier via archives)
- National Archives catalog (descriptions only)
- Census Bureau table listings
- All analysis and visualization features

**Requires Subscription:**
- HeinOnline (comprehensive legislative history)
- ProQuest Historical Newspapers (full news coverage)
- LexisNexis Academic (legal research)

## Extending the Toolkit

### Add New Data Sources

Create a new method in the `DisabilityDataRetriever` class:

```python
def search_new_source(self, query: str) -> List[Dict]:
    """Search a new data source"""
    # Implementation here
    results = []
    # ... your code ...
    return results
```

### Add Custom Analysis

Create new methods in the `StatisticalAnalyzer` class:

```python
def custom_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
    """Perform custom analysis"""
    # Your analysis code here
    return results_df
```

### Create Custom Visualizations

Add new plotting methods to the `DisabilityDataVisualizer` class:

```python
def plot_custom_chart(self, df: pd.DataFrame, save_name: str):
    """Create custom visualization"""
    # Your plotting code here
    plt.savefig(self.output_dir / save_name)
    plt.close()
```

## Troubleshooting

### Import Errors
```bash
# Install missing packages
pip install pandas requests beautifulsoup4 matplotlib seaborn
```

### API Errors
- Check your API key is valid
- Verify internet connection
- Ensure you're not hitting rate limits (toolkit includes delays)

### Empty Results
- Verify date ranges are correct (1975-2000)
- Check search terms are spelled correctly
- Some sources have limited historical digitization

### Memory Issues
- Process large datasets in chunks
- Use `chunksize` parameter in pandas for large CSV files
- Reduce date ranges for initial testing

## Best Practices

1. **Start Small**: Test with single queries before batch processing
2. **Save Regularly**: Results are automatically saved to avoid data loss
3. **Document Changes**: Keep notes on custom queries and modifications
4. **Respect Rate Limits**: Don't modify delay timings without good reason
5. **Verify Data**: Always check data quality reports before analysis

## Research Applications

This toolkit is ideal for:

- **Policy Analysis**: Track evolution of disability rights legislation
- **Historical Research**: Document disability rights movement
- **Quantitative Studies**: Analyze temporal trends and patterns
- **Data Journalism**: Create compelling visualizations
- **Academic Research**: Support dissertations and publications
- **Advocacy Work**: Evidence-based policy recommendations

## Citation

If you use this toolkit in research, please cite:

```
Disability Rights Data Retrieval & Analysis Toolkit (1975-2000)
Version 1.0, 2025
```

## Contributing

Contributions welcome! Areas for improvement:
- Additional data source integrations
- Enhanced NLP analysis
- More visualization types
- Performance optimizations
- Documentation improvements

## License

This toolkit is provided for academic and research purposes. Please respect:
- API terms of service for all data sources
- Copyright laws when working with retrieved documents
- Attribution requirements for data sources

## Support

For issues or questions:
1. Check this README thoroughly
2. Review code comments in source files
3. Test with demo data before using your own
4. Verify all dependencies are installed correctly

## Acknowledgments

Data sources:
- Government Publishing Office (GovInfo)
- National Archives and Records Administration
- U.S. Census Bureau
- Disability rights advocates and researchers

## Version History

**v1.0** (2025)
- Initial release
- Core retrieval functionality
- Advanced analysis module
- Comprehensive visualizations
- Documentation and examples

---

**Happy researching! 📊🔍**
