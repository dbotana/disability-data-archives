"""
Visualization Module for Disability Rights Data
Creates charts, graphs, and interactive visualizations
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)


class DisabilityDataVisualizer:
    """Create visualizations for disability rights data"""
    
    def __init__(self, data_dir: str = "disability_data", output_dir: str = "visualizations"):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def plot_timeline(self, df: pd.DataFrame, save_name: str = "timeline.png"):
        """Create timeline visualization of disability rights events"""
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Create figure
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Color map for event types
        type_colors = {
            'legislation': '#2E86AB',
            'regulation': '#A23B72',
            'activism': '#F18F01',
            'court_case': '#C73E1D'
        }
        
        # Plot events
        for i, row in df.iterrows():
            color = type_colors.get(row['type'], '#666666')
            ax.scatter(row['date'], i, c=color, s=200, zorder=2, alpha=0.8)
            ax.plot([row['date'], row['date']], [0, i], 'k--', alpha=0.3, linewidth=1)
            
            # Add label
            label = row['event'][:50] + '...' if len(row['event']) > 50 else row['event']
            ax.text(row['date'], i + 0.3, label, fontsize=9, 
                   verticalalignment='bottom', horizontalalignment='left')
        
        # Formatting
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Events', fontsize=12, fontweight='bold')
        ax.set_title('Disability Rights Timeline (1975-2000)', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_yticks([])
        ax.grid(axis='x', alpha=0.3)
        
        # Legend
        legend_elements = [plt.Line2D([0], [0], marker='o', color='w', 
                                     markerfacecolor=color, markersize=10, label=event_type.replace('_', ' ').title())
                          for event_type, color in type_colors.items()]
        ax.legend(handles=legend_elements, loc='upper left', fontsize=10)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / save_name, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved timeline visualization to {save_name}")
    
    def plot_yearly_trends(self, df: pd.DataFrame, save_name: str = "yearly_trends.png"):
        """Plot yearly trends in disability rights activity"""
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        
        # Count by year and type
        yearly_counts = df.groupby(['year', 'type']).size().reset_index(name='count')
        
        # Create figure
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        
        # Plot 1: Stacked bar chart
        pivot_data = yearly_counts.pivot(index='year', columns='type', values='count').fillna(0)
        pivot_data.plot(kind='bar', stacked=True, ax=axes[0], 
                       color=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])
        axes[0].set_title('Disability Rights Events by Year and Type', 
                         fontsize=14, fontweight='bold')
        axes[0].set_xlabel('Year', fontsize=12)
        axes[0].set_ylabel('Number of Events', fontsize=12)
        axes[0].legend(title='Event Type', bbox_to_anchor=(1.05, 1), loc='upper left')
        axes[0].grid(axis='y', alpha=0.3)
        
        # Plot 2: Line chart of total events
        total_by_year = df.groupby('year').size().reset_index(name='total')
        axes[1].plot(total_by_year['year'], total_by_year['total'], 
                    marker='o', linewidth=2, markersize=8, color='#2E86AB')
        axes[1].fill_between(total_by_year['year'], total_by_year['total'], 
                           alpha=0.3, color='#2E86AB')
        axes[1].set_title('Total Disability Rights Events Over Time', 
                         fontsize=14, fontweight='bold')
        axes[1].set_xlabel('Year', fontsize=12)
        axes[1].set_ylabel('Total Events', fontsize=12)
        axes[1].grid(axis='both', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / save_name, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved yearly trends visualization to {save_name}")
    
    def plot_event_type_distribution(self, df: pd.DataFrame, save_name: str = "event_types.png"):
        """Plot distribution of event types"""
        
        # Count by type
        type_counts = df['type'].value_counts()
        
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Pie chart
        colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#06A77D']
        explode = [0.05] * len(type_counts)
        ax1.pie(type_counts.values, labels=type_counts.index, autopct='%1.1f%%',
               colors=colors, explode=explode, shadow=True, startangle=90)
        ax1.set_title('Distribution of Event Types', fontsize=14, fontweight='bold')
        
        # Bar chart
        type_counts.plot(kind='barh', ax=ax2, color=colors[:len(type_counts)])
        ax2.set_title('Event Type Counts', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Number of Events', fontsize=12)
        ax2.set_ylabel('Event Type', fontsize=12)
        ax2.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / save_name, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved event type distribution to {save_name}")
    
    def plot_decade_comparison(self, df: pd.DataFrame, save_name: str = "decade_comparison.png"):
        """Compare disability rights activity across decades"""
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        df['decade'] = (df['year'] // 10) * 10
        
        # Count by decade and type
        decade_counts = df.groupby(['decade', 'type']).size().reset_index(name='count')
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Pivot for grouped bar chart
        pivot_data = decade_counts.pivot(index='decade', columns='type', values='count').fillna(0)
        pivot_data.plot(kind='bar', ax=ax, width=0.8,
                       color=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])
        
        ax.set_title('Disability Rights Activity by Decade (1975-2000)', 
                    fontsize=16, fontweight='bold')
        ax.set_xlabel('Decade', fontsize=12)
        ax.set_ylabel('Number of Events', fontsize=12)
        ax.legend(title='Event Type', bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(axis='y', alpha=0.3)
        ax.set_xticklabels([f"{int(x)}s" for x in pivot_data.index], rotation=0)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / save_name, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved decade comparison to {save_name}")
    
    def create_summary_dashboard(self, df: pd.DataFrame, save_name: str = "dashboard.png"):
        """Create comprehensive summary dashboard"""
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        
        # Create figure with multiple subplots
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # 1. Timeline (top row, full width)
        ax1 = fig.add_subplot(gs[0, :])
        yearly = df.groupby('year').size()
        ax1.plot(yearly.index, yearly.values, marker='o', linewidth=2, color='#2E86AB')
        ax1.fill_between(yearly.index, yearly.values, alpha=0.3, color='#2E86AB')
        ax1.set_title('Timeline of Events (1975-2000)', fontsize=12, fontweight='bold')
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Count')
        ax1.grid(alpha=0.3)
        
        # 2. Event types pie chart
        ax2 = fig.add_subplot(gs[1, 0])
        type_counts = df['type'].value_counts()
        ax2.pie(type_counts.values, labels=type_counts.index, autopct='%1.0f%%',
               startangle=90, colors=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])
        ax2.set_title('Event Types', fontsize=12, fontweight='bold')
        
        # 3. Events by decade
        ax3 = fig.add_subplot(gs[1, 1])
        df['decade'] = (df['year'] // 10) * 10
        decade_counts = df['decade'].value_counts().sort_index()
        ax3.bar(decade_counts.index, decade_counts.values, color='#A23B72', width=8)
        ax3.set_title('Events by Decade', fontsize=12, fontweight='bold')
        ax3.set_xlabel('Decade')
        ax3.set_ylabel('Count')
        ax3.set_xticks(decade_counts.index)
        ax3.set_xticklabels([f"{int(x)}s" for x in decade_counts.index])
        
        # 4. Key statistics
        ax4 = fig.add_subplot(gs[1, 2])
        ax4.axis('off')
        stats_text = f"""
        KEY STATISTICS
        
        Total Events: {len(df)}
        Date Range: {df['date'].min().year} - {df['date'].max().year}
        
        Event Types:
        {chr(10).join([f"  • {t}: {c}" for t, c in type_counts.items()])}
        
        Peak Year: {yearly.idxmax()} ({yearly.max()} events)
        """
        ax4.text(0.1, 0.5, stats_text, fontsize=10, family='monospace',
                verticalalignment='center')
        
        # 5. Cumulative events (bottom left)
        ax5 = fig.add_subplot(gs[2, 0])
        cumulative = df.groupby('year').size().cumsum()
        ax5.plot(cumulative.index, cumulative.values, linewidth=2, color='#F18F01')
        ax5.fill_between(cumulative.index, cumulative.values, alpha=0.3, color='#F18F01')
        ax5.set_title('Cumulative Events', fontsize=12, fontweight='bold')
        ax5.set_xlabel('Year')
        ax5.set_ylabel('Total Count')
        ax5.grid(alpha=0.3)
        
        # 6. Events per year heatmap
        ax6 = fig.add_subplot(gs[2, 1:])
        yearly_type = df.groupby(['year', 'type']).size().reset_index(name='count')
        pivot = yearly_type.pivot(index='type', columns='year', values='count').fillna(0)
        sns.heatmap(pivot, annot=False, fmt='g', cmap='YlOrRd', ax=ax6, cbar_kws={'label': 'Count'})
        ax6.set_title('Activity Heatmap by Type and Year', fontsize=12, fontweight='bold')
        ax6.set_xlabel('Year')
        ax6.set_ylabel('Event Type')
        
        plt.suptitle('Disability Rights Data Analysis Dashboard (1975-2000)', 
                    fontsize=16, fontweight='bold', y=0.995)
        
        plt.savefig(self.output_dir / save_name, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved summary dashboard to {save_name}")
    
    def export_to_html(self, df: pd.DataFrame, save_name: str = "interactive_timeline.html"):
        """Export interactive HTML visualization"""
        
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Disability Rights Timeline 1975-2000</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
                h1 {{ color: #2E86AB; }}
                .timeline {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .event {{ margin: 20px 0; padding: 15px; border-left: 4px solid #2E86AB; background: #f9f9f9; }}
                .event.legislation {{ border-left-color: #2E86AB; }}
                .event.regulation {{ border-left-color: #A23B72; }}
                .event.activism {{ border-left-color: #F18F01; }}
                .event.court_case {{ border-left-color: #C73E1D; }}
                .date {{ font-weight: bold; color: #666; }}
                .type {{ display: inline-block; padding: 3px 8px; border-radius: 3px; font-size: 12px; margin-left: 10px; }}
                .type.legislation {{ background: #2E86AB; color: white; }}
                .type.regulation {{ background: #A23B72; color: white; }}
                .type.activism {{ background: #F18F01; color: white; }}
                .type.court_case {{ background: #C73E1D; color: white; }}
                .impact {{ color: #555; margin-top: 8px; font-style: italic; }}
            </style>
        </head>
        <body>
            <h1>Disability Rights Timeline (1975-2000)</h1>
            <div class="timeline">
        """
        
        for _, row in df.iterrows():
            html_template += f"""
                <div class="event {row['type']}">
                    <div>
                        <span class="date">{row['date']}</span>
                        <span class="type {row['type']}">{row['type'].replace('_', ' ').title()}</span>
                    </div>
                    <h3>{row['event']}</h3>
                    <div class="impact">{row['impact']}</div>
                </div>
            """
        
        html_template += """
            </div>
        </body>
        </html>
        """
        
        with open(self.output_dir / save_name, 'w') as f:
            f.write(html_template)
        
        logger.info(f"Saved interactive HTML to {save_name}")


def create_all_visualizations(data_dir: str = "disability_data"):
    """Generate all visualizations"""
    
    print("=" * 70)
    print("Disability Rights Data Visualization")
    print("=" * 70)
    print()
    
    visualizer = DisabilityDataVisualizer(data_dir)
    
    # Load timeline data
    timeline_file = Path(data_dir) / "disability_timeline.csv"
    
    if not timeline_file.exists():
        print(f"❌ Timeline file not found: {timeline_file}")
        print("   Run disability_data_retrieval.py first")
        return
    
    print("📊 Loading timeline data...")
    df = pd.read_csv(timeline_file)
    print(f"   ✓ Loaded {len(df)} events")
    print()
    
    print("🎨 Creating visualizations...")
    
    # Generate all visualizations
    visualizer.plot_timeline(df)
    print("   ✓ Created timeline visualization")
    
    visualizer.plot_yearly_trends(df)
    print("   ✓ Created yearly trends chart")
    
    visualizer.plot_event_type_distribution(df)
    print("   ✓ Created event type distribution")
    
    visualizer.plot_decade_comparison(df)
    print("   ✓ Created decade comparison")
    
    visualizer.create_summary_dashboard(df)
    print("   ✓ Created summary dashboard")
    
    visualizer.export_to_html(df)
    print("   ✓ Created interactive HTML timeline")
    
    print()
    print("✅ All visualizations created!")
    print(f"📁 Visualizations saved to: {visualizer.output_dir}")
    print()
    print("Generated files:")
    print("  - timeline.png: Event timeline")
    print("  - yearly_trends.png: Yearly trend analysis")
    print("  - event_types.png: Event type distribution")
    print("  - decade_comparison.png: Decade-by-decade comparison")
    print("  - dashboard.png: Comprehensive summary dashboard")
    print("  - interactive_timeline.html: Interactive web view")


if __name__ == "__main__":
    create_all_visualizations()
