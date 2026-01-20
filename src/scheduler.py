import argparse
import time
import schedule
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys

# Add parent directory to path for imports when running as script
if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).parent.parent))

# Import Collectors
from src.collectors.mfds_collector import MFDSCollector
from src.collectors.fda_collector import FDACollector
from src.collectors.rasff_scraper import RASFFCollector
from src.collectors.impfood_scraper import ImpFoodScraper

# Import Utils
from src.utils.deduplication import merge_and_deduplicate
from src.utils.storage import save_to_parquet

class DataIngestionScheduler:
    """
    중앙 데이터 수집 스케줄러
    - 모든 수집기(MFDS, FDA, RASFF)를 순차적으로 실행
    - 수집 -> 정제 -> 중복제거 -> 저장 파이프라인 관리
    """
    
    def __init__(self, data_dir: Path = Path("data/hub")):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 수집기 인스턴스 초기화
        self.collectors = {
            "MFDS": MFDSCollector(),
            "FDA": FDACollector(),
            "RASFF": RASFFCollector(),
            "ImpFood": ImpFoodScraper()
        }

    def run_single_collector(self, name):
        """단일 수집기 실행 및 저장"""
        logger.info(f"🚀 Starting Collector: {name}")
        try:
            collector = self.collectors[name]
            
            # 수집 실행 (각 클래스의 메인 메서드 호출)
            if name in ["RASFF", "ImpFood"]:
                df = collector.scrape() # RASFF와 ImpFood는 scrape() 메서드 사용
            else:
                df = collector.collect() # 나머지는 collect() 사용
                
            if df.empty:
                logger.info(f"⚠️ {name}: No data collected.")
                return 0
                
            # 중복 제거
            df_new = merge_and_deduplicate(df, self.data_dir)
            
            # 저장
            count = save_to_parquet(df_new, self.data_dir, name)
            logger.success(f"✅ {name}: {count} new records saved.")
            return count
            
        except Exception as e:
            logger.error(f"❌ {name} Failed: {e}")
            return 0

    def run_all_collectors(self):
        """모든 수집기 순차 실행"""
        logger.info("🔄 Running ALL Collectors...")
        total_new = 0
        for name in self.collectors:
            total_new += self.run_single_collector(name)
        logger.success(f"🎉 Pipeline Finished. Total new records: {total_new}")
        return total_new

def job():
    """스케줄러에 등록될 작업"""
    print(f"\n[Scheduler] Job started at {datetime.now()}")
    scheduler = DataIngestionScheduler()
    scheduler.run_all_collectors()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "schedule"], required=True, help="Run mode")
    parser.add_argument("--time", default="09:00", help="Time to run in schedule mode (HH:MM)")
    parser.add_argument("--days", type=int, default=7, help="Days back (not used in all collectors)")
    
    args = parser.parse_args()
    
    if args.mode == "once":
        job()
    elif args.mode == "schedule":
        logger.info(f"⏰ Scheduler started. Running daily at {args.time}")
        schedule.every().day.at(args.time).do(job)
        
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == "__main__":
    main()