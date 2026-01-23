import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi
from pymongo.errors import BulkWriteError
import pytz
from dotenv import load_dotenv

# Create logs directory
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Configure logging with both file and console handlers
def setup_logging():
    """Setup logging configuration with file and console handlers"""
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    
    # File handler - daily log file
    log_filename = LOGS_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

SEASON_FIELDS = [f"season_{i}" for i in range(1, 16)]


@dataclass
class PipelineStats:
    """Statistics for pipeline execution"""
    collection: str
    old_db_count: int
    api_records: int
    new_records: int
    clean_records: int
    invalid_records: int
    inserted: int
    updated: int
    new_db_count: int
    errors: int = 0
    
    def __str__(self):
        return (f"{self.collection:<30} | Old DB: {self.old_db_count:>6} | "
                f"API: {self.api_records:>5} | New: {self.new_records:>5} | "
                f"Clean: {self.clean_records:>5} | Invalid: {self.invalid_records:>4} | "
                f"Inserted: {self.inserted:>5} | Updated: {self.updated:>5} | "
                f"New DB: {self.new_db_count:>6}")


class DataValidator:
    """Validates and cleans records based on content type"""
    
    @staticmethod
    def is_valid_movie(record: dict) -> bool:
        """Validate movie record"""
        if not isinstance(record, dict):
            return False
        return (
            record.get("featured_image") is not None and
            record.get("links") is not None
        )
    
    @staticmethod
    def is_valid_series(record: dict) -> bool:
        """Validate series record"""
        if not isinstance(record, dict):
            return False
        
        if record.get("featured_image") is None:
            return False
        
        # At least one season must have valid data
        return any(
            record.get(season) not in (None, "", "null")
            for season in SEASON_FIELDS
        )
    
    @classmethod
    def validate_records(cls, records: List[dict], content_type: str) -> Tuple[List[dict], int]:
        """
        Validate records based on content type
        Returns: (clean_records, invalid_count)
        """
        validator = cls.is_valid_movie if content_type == "movie" else cls.is_valid_series
        clean_records = [rec for rec in records if validator(rec)]
        invalid_count = len(records) - len(clean_records)
        return clean_records, invalid_count


class StreamlinedDataPipeline:
    """
    Modern data pipeline that operates entirely in-memory.
    No file operations, optimized for time, space, and resources.
    """
    
    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        db_name: str = "hdvideos",
        batch_size: int = 1000,
        max_workers: int = 4,
        api_timeout: int = 30
    ):
        # Initialize MongoDB connection
        if mongo_uri is None:
            load_dotenv()
            mongo_uri = os.getenv("Database_connection_key")
            if not mongo_uri:
                raise RuntimeError("Database_connection_key not found in environment")
        
        self.client = MongoClient(mongo_uri, server_api=ServerApi("1"))
        self.client.admin.command("ping")
        logger.info("✅ MongoDB connected successfully")
        
        self.db = self.client[db_name]
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.api_timeout = api_timeout
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # Log pipeline initialization
        logger.info(f"\n{'='*145}")
        logger.info(f"Pipeline initialized with:")
        logger.info(f"   • Database: {db_name}")
        logger.info(f"   • Batch size: {batch_size:,}")
        logger.info(f"   • Max workers: {max_workers}")
        logger.info(f"   • API timeout: {api_timeout}s")
        logger.info(f"   • Log file: logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log")
        logger.info(f"{'='*145}\n")
    
    def _get_ist_time(self) -> str:
        """Get current time in IST"""
        return datetime.now(self.ist).strftime("%Y-%m-%d %H:%M:%S")
    
    def _get_collection_count(self, collection_name: str) -> int:
        """Get total document count in collection"""
        try:
            collection = self.db[collection_name]
            return collection.count_documents({})
        except Exception as e:
            logger.error(f"Error getting count for {collection_name}: {e}")
            return 0
    
    def _get_last_modified_date(self, collection_name: str) -> Optional[datetime]:
        """Fetch the latest modified date from collection"""
        try:
            collection = self.db[collection_name]
            doc = collection.find_one(
                sort=[("modified_date", -1)],
                projection={"modified_date": 1}
            )
            
            if doc and "modified_date" in doc:
                mod_date = doc["modified_date"]
                if isinstance(mod_date, str):
                    return datetime.fromisoformat(mod_date.replace("Z", "+00:00"))
                return mod_date
            return None
        except Exception as e:
            logger.error(f"Error getting last modified date for {collection_name}: {e}")
            return None
    
    def _fetch_api_data(self, api_url: str) -> Optional[List[dict]]:
        """Fetch data from API"""
        try:
            response = requests.get(api_url, timeout=self.api_timeout)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"API fetch error from {api_url}: {e}")
            return None
    
    def _filter_new_records(
        self,
        records: List[dict],
        last_modified: Optional[datetime]
    ) -> List[dict]:
        """Filter records newer than last_modified date"""
        if last_modified is None:
            return records
        
        new_records = []
        for rec in records:
            try:
                mod_date_str = rec.get("modified_date")
                if not mod_date_str:
                    continue
                
                mod_date = datetime.fromisoformat(mod_date_str.replace("Z", "+00:00"))
                if mod_date > last_modified:
                    new_records.append(rec)
            except (ValueError, AttributeError) as e:
                logger.debug(f"Invalid date format in record: {e}")
                continue
        
        return new_records
    
    # def _upsert_records(
    #     self,
    #     collection_name: str,
    #     records: List[dict]
    # ) -> Tuple[int, int, int]:
    #     """
    #     Upsert records to MongoDB in batches
    #     Returns: (inserted_count, updated_count, error_count)
    #     """
    #     if not records:
    #         return 0, 0, 0
        
    #     collection = self.db[collection_name]
    #     inserted, updated, errors = 0, 0, 0
        
    #     # Process in batches
    #     for i in range(0, len(records), self.batch_size):
    #         batch = records[i:i + self.batch_size]
    #         operations = [
    #             UpdateOne(
    #                 {"record_id": rec["record_id"]},
    #                 {"$set": rec},
    #                 upsert=True
    #             )
    #             for rec in batch
    #             if rec.get("record_id")
    #         ]
            
    #         if not operations:
    #             continue
            
    #         try:
    #             result = collection.bulk_write(operations, ordered=False)
    #             inserted += result.upserted_count
    #             updated += result.modified_count
    #         except BulkWriteError as e:
    #             logger.error(f"Bulk write error for {collection_name}: {e.details}")
    #             errors += len(e.details.get('writeErrors', []))
    #         except Exception as e:
    #             logger.error(f"Unexpected error during upsert for {collection_name}: {e}")
    #             errors += len(operations)
        
    #     return inserted, updated, errors
    def _upsert_records(
    self,
    collection_name: str,
    records: List[dict]
    ) -> Tuple[int, int, int]:
        """
        Upsert records to MongoDB in batches
        Returns: (inserted_count, updated_count, error_count)
        """
        if not records:
            return 0, 0, 0
        
        collection = self.db[collection_name]
        inserted, updated, errors = 0, 0, 0
        
        # Process in batches
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            operations = []
            
            for rec in batch:
                if not rec.get("record_id"):
                    continue
                
                # Create a copy of the record without _id field
                update_data = {k: v for k, v in rec.items() if k != '_id'}
                
                operations.append(
                    UpdateOne(
                        {"record_id": rec["record_id"]},
                        {"$set": update_data},
                        upsert=True
                    )
                )
            
            if not operations:
                continue
            
            try:
                result = collection.bulk_write(operations, ordered=False)
                inserted += result.upserted_count
                updated += result.modified_count
            except BulkWriteError as e:
                logger.error(f"Bulk write error for {collection_name}: {e.details}")
                errors += len(e.details.get('writeErrors', []))
            except Exception as e:
                logger.error(f"Unexpected error during upsert for {collection_name}: {e}")
                errors += len(operations)
    
        return inserted, updated, errors
    def _process_collection(
        self,
        collection_name: str,
        api_url: str,
        content_type: str
    ) -> PipelineStats:
        """Process a single collection end-to-end"""
        
        logger.info(f"🔄 Processing: {collection_name}")
        
        # Step 0: Get current DB count
        old_db_count = self._get_collection_count(collection_name)
        logger.info(f"   Current DB records: {old_db_count}")
        
        # Step 1: Get last modified date from DB
        last_modified = self._get_last_modified_date(collection_name)
        if last_modified:
            logger.info(f"   Last modified in DB: {last_modified}")
        
        # Step 2: Fetch data from API
        api_records = self._fetch_api_data(api_url)
        if api_records is None:
            logger.warning(f"⚠️  Failed to fetch data for {collection_name}")
            return PipelineStats(
                collection=collection_name,
                old_db_count=old_db_count,
                api_records=0, new_records=0, clean_records=0,
                invalid_records=0, inserted=0, updated=0,
                new_db_count=old_db_count, errors=1
            )
        
        # Step 3: Filter new records
        new_records = self._filter_new_records(api_records, last_modified)
        
        if not new_records:
            logger.info(f"   ✓ No new records found")
            return PipelineStats(
                collection=collection_name,
                old_db_count=old_db_count,
                api_records=len(api_records), new_records=0, clean_records=0,
                invalid_records=0, inserted=0, updated=0,
                new_db_count=old_db_count
            )
        
        # Step 4: Validate and clean records
        clean_records, invalid_count = DataValidator.validate_records(
            new_records, content_type
        )
        
        if not clean_records:
            logger.info(f"   ⚠️  All {len(new_records)} new records were invalid")
            return PipelineStats(
                collection=collection_name,
                old_db_count=old_db_count,
                api_records=len(api_records), new_records=len(new_records),
                clean_records=0, invalid_records=invalid_count,
                inserted=0, updated=0,
                new_db_count=old_db_count
            )
        
        # Step 5: Upsert to MongoDB
        inserted, updated, errors = self._upsert_records(collection_name, clean_records)
        
        # Step 6: Get new DB count
        new_db_count = self._get_collection_count(collection_name)
        
        stats = PipelineStats(
            collection=collection_name,
            old_db_count=old_db_count,
            api_records=len(api_records),
            new_records=len(new_records),
            clean_records=len(clean_records),
            invalid_records=invalid_count,
            inserted=inserted,
            updated=updated,
            new_db_count=new_db_count,
            errors=errors
        )
        
        logger.info(f"   ✅ Completed: {inserted} inserted, {updated} updated, {invalid_count} invalid")
        logger.info(f"   📊 DB count: {old_db_count} → {new_db_count} (Change: +{new_db_count - old_db_count})")
        return stats
    
    def run_pipeline(self, api_config: Dict[str, Tuple[str, str]]) -> List[PipelineStats]:
        """
        Run the complete pipeline for all collections
        
        Args:
            api_config: Dict mapping collection_name -> (api_url, content_type)
                       content_type should be either "movie" or "series"
        
        Returns:
            List of PipelineStats for each collection
        """
        start_time = self._get_ist_time()
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 Data Pipeline Started at {start_time} IST")
        logger.info(f"{'='*80}\n")
        
        all_stats = []
        
        # Process collections concurrently
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._process_collection,
                    collection_name,
                    api_url,
                    content_type
                ): collection_name
                for collection_name, (api_url, content_type) in api_config.items()
            }
            
            for future in as_completed(futures):
                try:
                    stats = future.result()
                    all_stats.append(stats)
                except Exception as e:
                    collection = futures[future]
                    logger.error(f"❌ Error processing {collection}: {e}")
                    old_count = self._get_collection_count(collection)
                    all_stats.append(PipelineStats(
                        collection=collection,
                        old_db_count=old_count,
                        api_records=0, new_records=0, clean_records=0,
                        invalid_records=0, inserted=0, updated=0,
                        new_db_count=old_count, errors=1
                    ))
        
        # Print summary
        self._print_summary(all_stats, start_time)
        
        return all_stats
#------------------------telegram bot arena----------------------------------------------------
    @staticmethod
    def send_telegram_message(message: str):
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id_env = os.getenv("ALLOWED_USER_ID")  # using what you already have
        
        if not token or not chat_id_env:
            logger.warning("Telegram credentials not set. Skipping Telegram notification.")
            return
        chat_id = int(chat_id_env)
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }

        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info("📨 Daily summary sent to Telegram")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")

    
    def _print_summary(self, all_stats: List[PipelineStats], start_time: str):
        """Print pipeline execution summary"""
        end_time = self._get_ist_time()
        
        # Calculate totals
        total_old_db = sum(s.old_db_count for s in all_stats)
        total_api = sum(s.api_records for s in all_stats)
        total_new = sum(s.new_records for s in all_stats)
        total_clean = sum(s.clean_records for s in all_stats)
        total_invalid = sum(s.invalid_records for s in all_stats)
        total_inserted = sum(s.inserted for s in all_stats)
        total_updated = sum(s.updated for s in all_stats)
        total_new_db = sum(s.new_db_count for s in all_stats)
        total_errors = sum(s.errors for s in all_stats)
        total_change = total_new_db - total_old_db
        
        # Create professional header
        logger.info(f"\n{'='*145}")
        logger.info("📊 PIPELINE EXECUTION SUMMARY")
        logger.info(f"{'='*145}")
        
        # Column headers with full names
        header = (
            f"{'Collection Name':<35} | "
            f"{'Old DB':<8} | "
            f"{'API Data':<8} | "
            f"{'New':<6} | "
            f"{'Clean':<6} | "
            f"{'Invalid':<7} | "
            f"{'Inserted':<8} | "
            f"{'Updated':<7} | "
            f"{'New DB':<8} | "
            f"{'Change':<7}"
        )
        logger.info(header)
        logger.info(f"{'-'*145}")
        
        # Print each collection's stats
        for stats in sorted(all_stats, key=lambda x: x.collection):
            change = stats.new_db_count - stats.old_db_count
            change_str = f"+{change}" if change > 0 else str(change)
            
            row = (
                f"{stats.collection:<35} | "
                f"{stats.old_db_count:<8,} | "
                f"{stats.api_records:<8,} | "
                f"{stats.new_records:<6,} | "
                f"{stats.clean_records:<6,} | "
                f"{stats.invalid_records:<7,} | "
                f"{stats.inserted:<8,} | "
                f"{stats.updated:<7,} | "
                f"{stats.new_db_count:<8,} | "
                f"{change_str:<7}"
            )
            logger.info(row)
        
        # Print totals
        logger.info(f"{'-'*145}")
        change_str = f"+{total_change}" if total_change > 0 else str(total_change)
        totals = (
            f"{'TOTALS':<35} | "
            f"{total_old_db:<8,} | "
            f"{total_api:<8,} | "
            f"{total_new:<6,} | "
            f"{total_clean:<6,} | "
            f"{total_invalid:<7,} | "
            f"{total_inserted:<8,} | "
            f"{total_updated:<7,} | "
            f"{total_new_db:<8,} | "
            f"{change_str:<7}"
        )
        logger.info(totals)
        logger.info(f"{'='*145}")
        
        # Execution time and status
        logger.info(f"⏱️  Started:  {start_time} IST")
        logger.info(f"⏱️  Finished: {end_time} IST")
        
        if total_errors > 0:
            logger.warning(f"⚠️  Errors: {total_errors}")
        
        # Summary statistics
        logger.info(f"\n📈 Summary Statistics:")
        logger.info(f"   • Total records processed from API: {total_api:,}")
        logger.info(f"   • New records identified: {total_new:,}")
        logger.info(f"   • Clean records validated: {total_clean:,}")
        logger.info(f"   • Invalid records rejected: {total_invalid:,}")
        logger.info(f"   • Database insertions: {total_inserted:,}")
        logger.info(f"   • Database updates: {total_updated:,}")
        logger.info(f"   • Net database change: {change_str}")
        logger.info(f"   • Overall database size: {total_old_db:,} → {total_new_db:,}")
        
        logger.info(f"\n✅ Pipeline execution completed successfully")
        logger.info(f"📝 Full log saved to: logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log\n")
                # --- Telegram summary (summary only, no logs) ---
        telegram_summary = (
            "📈 <b>Daily Pipeline Summary</b>\n\n"
            f"• Total records processed from API: <b>{total_api:,}</b>\n"
            f"• New records identified: <b>{total_new:,}</b>\n"
            f"• Clean records validated: <b>{total_clean:,}</b>\n"
            f"• Invalid records rejected: <b>{total_invalid:,}</b>\n"
            f"• Database insertions: <b>{total_inserted:,}</b>\n"
            f"• Database updates: <b>{total_updated:,}</b>\n"
            f"• Net database change: <b>{change_str}</b>\n"
            f"• Overall database size: <b>{total_old_db:,} → {total_new_db:,}</b>\n\n"
            f"⏱️ Finished at: <code>{end_time} IST</code>"
        )

        self.send_telegram_message(telegram_summary)


    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")



def main():
    """Main execution function"""
    
    # Configuration: collection_name -> (api_url, content_type)
    api_config = {
        "hollywood_movie_arena": (
            "https://api.hicine.info/api/hollywood_movies?offset=1000&limit=10000",
            "movie"
        ),
        "hollywood_series_arena": (
            "https://api.hicine.info/api/hollywood_series?offset=1000&limit=10000",
            "series"
        ),
        "bollywood_series_arena": (
            "https://api.hicine.info/api/bollywood_series?offset=1000&limit=10000",
            "series"
        ),
        "bollywood_movie_arena": (
            "https://api.hicine.info/api/bollywood_movies?offset=18000&limit=5500",
            "movie"
        ),
        "anieme_arena": (
            "https://api.hicine.info/api/anime?offset=1000&limit=1000",
            "series"
        )
    }
    
    # Initialize and run pipeline
    pipeline = StreamlinedDataPipeline(
        batch_size=1000,
        max_workers=4,
        api_timeout=30
    )
    
    try:
        pipeline.run_pipeline(api_config)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
