"""
Disability Rights Data Retrieval & Analysis Toolkit (1975-2000)
Automates collection and analysis of historical disability policy data
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, List, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DisabilityDataRetriever:
    """Main class for retrieving disability rights data from various sources"""
    
    def __init__(self, output_dir: str = "disability_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DisabilityResearchBot/1.0 (Academic Research)'
        })
        
    def search_govinfo(self, 
                       query: str, 
                       start_date: str = "1975-01-01",
                       end_date: str = "2000-12-31",
                       collection: str = "FR") -> List[Dict]:
        """
        Search GovInfo API for disability-related documents
        
        Args:
            query: Search terms
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            collection: Collection code (FR=Federal Register, CREC=Congressional Record)
        
        Returns:
            List of document metadata
        """
        base_url = "https://api.govinfo.gov/search"
        params = {
            'query': query,
            'collection': collection,
            'publishedDate': f'{start_date}:{end_date}',
            'pageSize': 100,
            'offsetMark': '*',
            'api_key': 'DEMO'  # Replace with actual API key from api.data.gov
        }
        
        results = []
        try:
            logger.info(f"Searching GovInfo for: {query} ({start_date} to {end_date})")
            response = self.session.get(base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                logger.info(f"Found {len(results)} results")
            else:
                logger.warning(f"GovInfo API returned status {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error searching GovInfo: {e}")
            
        return results
    
    def download_govinfo_document(self, package_id: str, format_type: str = "pdf") -> Optional[bytes]:
        """Download document content from GovInfo"""
        url = f"https://api.govinfo.gov/packages/{package_id}/{format_type}"
        params = {'api_key': 'DEMO'}
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            if response.status_code == 200:
                return response.content
        except Exception as e:
            logger.error(f"Error downloading {package_id}: {e}")
        
        return None
    
    def scrape_census_disability_tables(self) -> pd.DataFrame:
        """
        Scrape Census Bureau disability statistics
        Note: Historical tables may require manual download
        """
        # Census disability data endpoints
        urls = [
            "https://www.census.gov/topics/health/disability/data/tables.html",
            "https://data.census.gov/table"
        ]
        
        all_tables = []
        
        for url in urls:
            try:
                logger.info(f"Scraping Census data from {url}")
                response = self.session.get(url, timeout=30)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find table links
                links = soup.find_all('a', href=re.compile(r'\.xls|\.xlsx|\.csv'))
                
                for link in links:
                    table_info = {
                        'title': link.get_text(strip=True),
                        'url': link['href'],
                        'source': 'Census Bureau'
                    }
                    all_tables.append(table_info)
                    
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
        
        return pd.DataFrame(all_tables)
    
    def search_archives_catalog(self, query: str) -> List[Dict]:
        """
        Search National Archives Catalog
        Uses the public API
        """
        base_url = "https://catalog.archives.gov/api/v1/"
        params = {
            'q': query,
            'rows': 100,
            'resultTypes': 'item,fileUnit',
            'sort': 'naIdSort asc'
        }
        
        results = []
        try:
            logger.info(f"Searching National Archives for: {query}")
            response = self.session.get(base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'opaResponse' in data:
                    results = data['opaResponse'].get('results', {}).get('result', [])
                    logger.info(f"Found {len(results)} archive records")
                    
        except Exception as e:
            logger.error(f"Error searching Archives: {e}")
            
        return results
    
    def search_news_archive(self, query: str, year_range: tuple = (1975, 2000)) -> List[Dict]:
        """
        Search Google News Archive (limited functionality)
        Note: Google News Archive search was deprecated, this provides framework
        """
        results = []
        
        # Framework for news archive searching
        # Actual implementation would depend on available APIs or scraping permissions
        logger.info(f"News archive search for '{query}' from {year_range[0]}-{year_range[1]}")
        logger.info("Note: Direct Google News Archive API unavailable - consider ProQuest API")
        
        return results
    
    def compile_disability_timeline(self) -> pd.DataFrame:
        """Create comprehensive timeline of disability rights milestones"""
        
        milestones = [
            {
                'date': '1975-11-29',
                'event': 'Education for All Handicapped Children Act signed',
                'type': 'legislation',
                'impact': 'Guaranteed FAPE for children with disabilities'
            },
            {
                'date': '1977-04-28',
                'event': 'Section 504 regulations finalized',
                'type': 'regulation',
                'impact': 'First civil rights protection for people with disabilities'
            },
            {
                'date': '1977-04-05',
                'event': 'Section 504 sit-ins begin (HEW San Francisco)',
                'type': 'activism',
                'impact': 'Longest sit-in in federal building history (25 days)'
            },
            {
                'date': '1986-10-21',
                'event': 'Air Carrier Access Act signed',
                'type': 'legislation',
                'impact': 'Prohibited airline discrimination'
            },
            {
                'date': '1988-09-13',
                'event': 'Fair Housing Amendments Act signed',
                'type': 'legislation',
                'impact': 'Extended fair housing protections to people with disabilities'
            },
            {
                'date': '1990-07-26',
                'event': 'Americans with Disabilities Act signed',
                'type': 'legislation',
                'impact': 'Comprehensive civil rights law for people with disabilities'
            },
            {
                'date': '1990-10-30',
                'event': 'IDEA (renamed from EHA)',
                'type': 'legislation',
                'impact': 'Person-first language, added autism and TBI categories'
            },
            {
                'date': '1997-06-04',
                'event': 'IDEA Amendments of 1997',
                'type': 'legislation',
                'impact': 'Required general education curriculum access'
            },
            {
                'date': '1999-06-22',
                'event': 'Olmstead v. L.C. decision',
                'type': 'court_case',
                'impact': 'Right to community-based services'
            }
        ]
        
        df = pd.DataFrame(milestones)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        return df
    
    def save_results(self, data: pd.DataFrame, filename: str):
        """Save results to multiple formats"""
        base_path = self.output_dir / filename
        
        # Save as CSV
        data.to_csv(f"{base_path}.csv", index=False)
        logger.info(f"Saved to {base_path}.csv")
        
        # Save as JSON
        data.to_json(f"{base_path}.json", orient='records', indent=2)
        logger.info(f"Saved to {base_path}.json")
        
        # Save as Excel if multiple sheets needed
        if len(data) > 0:
            with pd.ExcelWriter(f"{base_path}.xlsx", engine='openpyxl') as writer:
                data.to_excel(writer, sheet_name='Data', index=False)
            logger.info(f"Saved to {base_path}.xlsx")


class DisabilityDataAnalyzer:
    """Analyze retrieved disability rights data"""
    
    def __init__(self, data_dir: str = "disability_data"):
        self.data_dir = Path(data_dir)
        
    def analyze_legislation_timeline(self, df: pd.DataFrame) -> Dict:
        """Analyze temporal patterns in disability legislation"""
        
        # Convert date column
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        df['decade'] = (df['year'] // 10) * 10
        
        analysis = {
            'total_events': len(df),
            'by_type': df['type'].value_counts().to_dict(),
            'by_decade': df['decade'].value_counts().sort_index().to_dict(),
            'by_year': df['year'].value_counts().sort_index().to_dict(),
            'key_periods': {
                '1975-1979': len(df[(df['year'] >= 1975) & (df['year'] <= 1979)]),
                '1980-1989': len(df[(df['year'] >= 1980) & (df['year'] <= 1989)]),
                '1990-1999': len(df[(df['year'] >= 1990) & (df['year'] <= 1999)])
            }
        }
        
        return analysis
    
    def analyze_document_corpus(self, documents: List[Dict]) -> pd.DataFrame:
        """Analyze corpus of retrieved documents"""
        
        if not documents:
            return pd.DataFrame()
        
        df = pd.DataFrame(documents)
        
        analysis = pd.DataFrame({
            'metric': [
                'Total Documents',
                'Unique Sources',
                'Date Range',
                'Average Title Length'
            ],
            'value': [
                len(df),
                df['source'].nunique() if 'source' in df else 0,
                f"{df['date'].min()} to {df['date'].max()}" if 'date' in df else 'N/A',
                df['title'].str.len().mean() if 'title' in df else 0
            ]
        })
        
        return analysis
    
    def extract_key_terms(self, documents: List[Dict], top_n: int = 20) -> pd.DataFrame:
        """Extract most common terms from document corpus"""
        
        from collections import Counter
        
        # Disability-related terms to track
        key_terms = [
            'disability', 'disabilities', 'disabled',
            'ADA', 'Americans with Disabilities Act',
            'Section 504', 'Rehabilitation Act',
            'IDEA', 'special education',
            'accessibility', 'accessible',
            'accommodation', 'reasonable accommodation',
            'discrimination',
            'independent living',
            'deinstitutionalization',
            'mainstreaming', 'inclusion'
        ]
        
        term_counts = Counter()
        
        for doc in documents:
            text = str(doc.get('title', '')) + ' ' + str(doc.get('content', ''))
            text_lower = text.lower()
            
            for term in key_terms:
                count = text_lower.count(term.lower())
                if count > 0:
                    term_counts[term] += count
        
        df = pd.DataFrame(term_counts.most_common(top_n), columns=['term', 'frequency'])
        return df
    
    def create_visualization_data(self, df: pd.DataFrame) -> Dict:
        """Prepare data for visualization"""
        
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        
        viz_data = {
            'timeline': df.groupby('year').size().to_dict(),
            'by_type': df.groupby('type').size().to_dict(),
            'events': df.to_dict('records')
        }
        
        return viz_data


def main():
    """Main execution function"""
    
    print("=" * 70)
    print("Disability Rights Data Retrieval & Analysis Toolkit (1975-2000)")
    print("=" * 70)
    print()
    
    # Initialize retriever
    retriever = DisabilityDataRetriever()
    analyzer = DisabilityDataAnalyzer()
    
    # 1. Create disability rights timeline
    print("📅 Creating disability rights timeline...")
    timeline_df = retriever.compile_disability_timeline()
    retriever.save_results(timeline_df, "disability_timeline")
    print(f"   ✓ Saved {len(timeline_df)} milestone events")
    print()
    
    # 2. Search GovInfo Federal Register
    print("🏛️  Searching GovInfo Federal Register...")
    queries = [
        "Americans with Disabilities Act",
        "Section 504",
        "disability rights",
        "special education"
    ]
    
    all_govinfo_results = []
    for query in queries:
        results = retriever.search_govinfo(query, "1975-01-01", "2000-12-31")
        all_govinfo_results.extend(results)
        print(f"   Found {len(results)} results for '{query}'")
        time.sleep(1)  # Rate limiting
    
    if all_govinfo_results:
        govinfo_df = pd.DataFrame(all_govinfo_results)
        retriever.save_results(govinfo_df, "govinfo_federal_register")
        print(f"   ✓ Saved {len(all_govinfo_results)} Federal Register documents")
    print()
    
    # 3. Search National Archives
    print("📚 Searching National Archives Catalog...")
    archive_queries = [
        "disability rights",
        "Section 504",
        "Americans with Disabilities Act",
        "special education"
    ]
    
    all_archive_results = []
    for query in archive_queries:
        results = retriever.search_archives_catalog(query)
        all_archive_results.extend(results)
        print(f"   Found {len(results)} records for '{query}'")
        time.sleep(1)
    
    if all_archive_results:
        archives_df = pd.DataFrame(all_archive_results)
        retriever.save_results(archives_df, "national_archives_records")
        print(f"   ✓ Saved {len(all_archive_results)} archive records")
    print()
    
    # 4. Scrape Census disability tables
    print("📊 Finding Census Bureau disability tables...")
    census_tables = retriever.scrape_census_disability_tables()
    if not census_tables.empty:
        retriever.save_results(census_tables, "census_disability_tables")
        print(f"   ✓ Found {len(census_tables)} Census tables")
    print()
    
    # 5. Analyze timeline
    print("📈 Analyzing disability rights timeline...")
    analysis = analyzer.analyze_legislation_timeline(timeline_df)
    
    print(f"   Total events: {analysis['total_events']}")
    print(f"   By type: {analysis['by_type']}")
    print(f"   By decade: {analysis['by_decade']}")
    print()
    
    # Save analysis
    analysis_df = pd.DataFrame([
        {'metric': 'Total Events', 'value': analysis['total_events']},
        {'metric': 'Legislation Count', 'value': analysis['by_type'].get('legislation', 0)},
        {'metric': 'Court Cases', 'value': analysis['by_type'].get('court_case', 0)},
        {'metric': 'Activism Events', 'value': analysis['by_type'].get('activism', 0)},
    ])
    retriever.save_results(analysis_df, "timeline_analysis")
    
    print("✅ Data retrieval and analysis complete!")
    print(f"📁 All results saved to: {retriever.output_dir}")
    print()
    print("Next steps:")
    print("  1. Review CSV/JSON files in the output directory")
    print("  2. Use the Excel files for further analysis")
    print("  3. Run advanced_analysis.py for deeper insights")
    print("  4. Get API keys for full GovInfo access (api.data.gov)")


if __name__ == "__main__":
    main()
