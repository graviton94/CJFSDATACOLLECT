"""
Daily scheduler for automated food safety data ingestion.
Orchestrates collection from all three sources with APScheduler.
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from pathlib import Path
from loguru import logger
import sys
import os
import yaml
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.rasff_scraper import RASFFScraper
from collectors.fda_collector import FDACollector
from collectors.mfds_collector import MFDSCollector


class DataIngestionScheduler:
    """Scheduler for daily data ingestion from all sources using APScheduler."""
    
    def __init__(self, data_dir: Path = None, days_back: int = 1, config_path: Path = None):
        """
        Initialize scheduler.
        
        Args:
            data_dir: Directory for storing processed data
            days_back: Number of days to look back in each run
            config_path: Path to config.yaml file
        """
        # Load environment variables
        load_dotenv()
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        self.data_dir = data_dir or Path(self.config.get('data', {}).get('output_dir', 'data/processed'))
        self.days_back = days_back
        
        # Initialize collectors
        self.rasff_scraper = RASFFScraper(data_dir=self.data_dir)
        self.fda_collector = FDACollector(data_dir=self.data_dir)
        
        # Get API key from environment
        api_key = os.getenv('MFDS_API_KEY')
        self.mfds_collector = MFDSCollector(api_key=api_key, data_dir=self.data_dir)
        
        # Initialize APScheduler
        self.scheduler = BackgroundScheduler()
        
    def _load_config(self, config_path: Path = None) -> dict:
        """
        Load configuration from config.yaml.
        
        Args:
            config_path: Path to config.yaml file
            
        Returns:
            Configuration dictionary
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded configuration from {config_path}")
                return config or {}
        except FileNotFoundError:
            logger.warning(f"Config file not found at {config_path}, using defaults")
            return {}
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return {}
        
    def run_all_collectors(self):
        """
        Run complete data collection pipeline: Collection → Validation → Deduplication → Storage.
        
        Returns:
            Total number of records collected
        """
        logger.info("=" * 60)
        logger.info(f"Starting data collection pipeline at {datetime.now()}")
        logger.info("=" * 60)
        
        total_records = 0
        
        # Step 1: Collect from all sources
        logger.info("STEP 1: Data Collection from all sources")
        logger.info("-" * 60)
        
        try:
            # Collect from EU RASFF
            logger.info("Collecting from EU RASFF...")
            rasff_count = self.rasff_scraper.collect_and_store(days_back=self.days_back)
            logger.info(f"✓ EU RASFF: {rasff_count} new records")
            total_records += rasff_count
            
        except Exception as e:
            logger.error(f"✗ Error collecting from EU RASFF: {e}")
        
        try:
            # Collect from FDA
            logger.info("Collecting from FDA Import Alerts...")
            fda_count = self.fda_collector.collect_and_store(days_back=self.days_back)
            logger.info(f"✓ FDA Import Alerts: {fda_count} new records")
            total_records += fda_count
            
        except Exception as e:
            logger.error(f"✗ Error collecting from FDA: {e}")
        
        try:
            # Collect from Korea MFDS
            logger.info("Collecting from Korea MFDS...")
            mfds_count = self.mfds_collector.collect_and_store(days_back=self.days_back)
            logger.info(f"✓ Korea MFDS: {mfds_count} new records")
            total_records += mfds_count
            
        except Exception as e:
            logger.error(f"✗ Error collecting from Korea MFDS: {e}")
        
        # Step 2: Validation (already handled by collect_and_store via schema validation)
        logger.info("-" * 60)
        logger.info("STEP 2: Validation - Completed during collection")
        
        # Step 3: Deduplication (already handled by collect_and_store via merge_and_deduplicate)
        logger.info("STEP 3: Deduplication - Completed during storage")
        
        # Step 4: Storage (already handled by collect_and_store)
        logger.info("STEP 4: Storage - Data saved to hub_data.parquet")
        
        logger.info("=" * 60)
        logger.info(f"✓ Pipeline complete: {total_records} total new records")
        logger.info(f"✓ Data available at: {self.data_dir / 'hub_data.parquet'}")
        logger.info("=" * 60)
        
        return total_records
    
    def run_single_collector(self, collector_name: str):
        """
        Run a single collector by name.
        
        Args:
            collector_name: Name of collector ('MFDS', 'FDA', or 'RASFF')
            
        Returns:
            Number of records collected
        """
        logger.info(f"Running {collector_name} collector...")
        
        try:
            if collector_name.upper() == 'MFDS':
                count = self.mfds_collector.collect_and_store(days_back=self.days_back)
            elif collector_name.upper() == 'FDA':
                count = self.fda_collector.collect_and_store(days_back=self.days_back)
            elif collector_name.upper() == 'RASFF':
                count = self.rasff_scraper.collect_and_store(days_back=self.days_back)
            else:
                raise ValueError(f"Unknown collector: {collector_name}")
            
            logger.info(f"✓ {collector_name}: {count} new records")
            return count
            
        except Exception as e:
            logger.error(f"✗ Error running {collector_name} collector: {e}")
            raise
    
    def schedule_daily(self, run_time: str = "09:00"):
        """
        Schedule daily ingestion using APScheduler.
        
        Args:
            run_time: Time to run daily (HH:MM format)
        """
        hour, minute = run_time.split(':')
        
        logger.info(f"Scheduling daily collection at {run_time}")
        
        # Add job with cron trigger
        self.scheduler.add_job(
            self.run_all_collectors,
            trigger=CronTrigger(hour=int(hour), minute=int(minute)),
            id='daily_collection',
            name='Daily Food Safety Data Collection',
            replace_existing=True
        )
        
        # Start the scheduler
        self.scheduler.start()
        
        logger.info("✓ APScheduler started. Press Ctrl+C to stop.")
        logger.info(f"✓ Next run scheduled at: {run_time}")
        
        try:
            # Keep the script running
            import time
            while True:
                time.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down scheduler...")
            self.scheduler.shutdown()
            logger.info("✓ Scheduler stopped")
    
    def run_once(self):
        """Run collection pipeline once immediately."""
        return self.run_all_collectors()
    
    def get_scheduler_status(self):
        """
        Get the status of scheduled jobs.
        
        Returns:
            List of job information dictionaries
        """
        if not self.scheduler.running:
            return []
        
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time,
                'trigger': str(job.trigger)
            })
        return jobs


def main():
    """Main entry point for scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Food safety data ingestion scheduler with APScheduler')
    parser.add_argument('--mode', choices=['once', 'schedule'], default='once',
                       help='Run mode: once (immediate run) or schedule (daily scheduled runs)')
    parser.add_argument('--time', type=str, default='09:00',
                       help='Scheduled run time in HH:MM format (default: 09:00)')
    parser.add_argument('--days', type=int, default=1,
                       help='Days to look back for data collection (default: 1)')
    parser.add_argument('--data-dir', type=Path, default=None,
                       help='Output directory for processed data (default: from config.yaml)')
    parser.add_argument('--config', type=Path, default=None,
                       help='Path to config.yaml file')
    
    args = parser.parse_args()
    
    # Initialize scheduler
    scheduler = DataIngestionScheduler(
        data_dir=args.data_dir,
        days_back=args.days,
        config_path=args.config
    )
    
    if args.mode == 'once':
        logger.info("Running collection pipeline once...")
        count = scheduler.run_once()
        logger.info(f"✓ Completed: {count} new records collected")
    else:
        logger.info(f"Starting scheduled mode - daily runs at {args.time}")
        scheduler.schedule_daily(run_time=args.time)


if __name__ == '__main__':
    main()
