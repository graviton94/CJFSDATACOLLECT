"""
Daily scheduler for automated food safety data ingestion.
Orchestrates collection from all three sources.
"""

import schedule
import time
from datetime import datetime
from pathlib import Path
from loguru import logger
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.rasff_scraper import RASFFScraper
from collectors.fda_collector import FDACollector
from collectors.mfds_collector import MFDSCollector


class DataIngestionScheduler:
    """Scheduler for daily data ingestion from all sources."""
    
    def __init__(self, data_dir: Path = None, days_back: int = 1):
        """
        Initialize scheduler.
        
        Args:
            data_dir: Directory for storing processed data
            days_back: Number of days to look back in each run
        """
        self.data_dir = data_dir or Path("data/processed")
        self.days_back = days_back
        
        # Initialize collectors
        self.rasff_scraper = RASFFScraper(data_dir=self.data_dir)
        self.fda_collector = FDACollector(data_dir=self.data_dir)
        self.mfds_collector = MFDSCollector(data_dir=self.data_dir)
        
    def run_ingestion(self):
        """Run data ingestion from all sources."""
        logger.info("=" * 60)
        logger.info(f"Starting scheduled ingestion at {datetime.now()}")
        logger.info("=" * 60)
        
        total_records = 0
        
        try:
            # Collect from EU RASFF
            logger.info("Collecting from EU RASFF...")
            rasff_count = self.rasff_scraper.collect_and_store(days_back=self.days_back)
            logger.info(f"EU RASFF: {rasff_count} new records")
            total_records += rasff_count
            
        except Exception as e:
            logger.error(f"Error collecting from EU RASFF: {e}")
        
        try:
            # Collect from FDA
            logger.info("Collecting from FDA Import Alerts...")
            fda_count = self.fda_collector.collect_and_store(days_back=self.days_back)
            logger.info(f"FDA Import Alerts: {fda_count} new records")
            total_records += fda_count
            
        except Exception as e:
            logger.error(f"Error collecting from FDA: {e}")
        
        try:
            # Collect from Korea MFDS
            logger.info("Collecting from Korea MFDS...")
            mfds_count = self.mfds_collector.collect_and_store(days_back=self.days_back)
            logger.info(f"Korea MFDS: {mfds_count} new records")
            total_records += mfds_count
            
        except Exception as e:
            logger.error(f"Error collecting from Korea MFDS: {e}")
        
        logger.info("=" * 60)
        logger.info(f"Ingestion complete: {total_records} total new records")
        logger.info("=" * 60)
        
        return total_records
    
    def schedule_daily(self, run_time: str = "02:00"):
        """
        Schedule daily ingestion.
        
        Args:
            run_time: Time to run daily (HH:MM format)
        """
        logger.info(f"Scheduling daily ingestion at {run_time}")
        
        schedule.every().day.at(run_time).do(self.run_ingestion)
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
    
    def run_once(self):
        """Run ingestion once immediately."""
        return self.run_ingestion()


def main():
    """Main entry point for scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Food safety data ingestion scheduler')
    parser.add_argument('--mode', choices=['once', 'schedule'], default='once',
                       help='Run mode: once or schedule')
    parser.add_argument('--time', type=str, default='02:00',
                       help='Scheduled run time (HH:MM)')
    parser.add_argument('--days', type=int, default=1,
                       help='Days to look back')
    parser.add_argument('--data-dir', type=Path, default=Path('data/processed'),
                       help='Output directory')
    
    args = parser.parse_args()
    
    scheduler = DataIngestionScheduler(
        data_dir=args.data_dir,
        days_back=args.days
    )
    
    if args.mode == 'once':
        logger.info("Running ingestion once")
        count = scheduler.run_once()
        logger.info(f"Completed: {count} new records")
    else:
        logger.info("Starting scheduled mode")
        scheduler.schedule_daily(run_time=args.time)


if __name__ == '__main__':
    main()
