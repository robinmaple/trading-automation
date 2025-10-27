import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import logging
from typing import List, Optional, Tuple

# Configuration
DATA_FOLDER = r"C:\Robin\Data\Daily"
LOG_FOLDER = r"C:\Robin\Data"
TICKER_FILE = r"C:\Robin\Data\ticker_list.txt"
BATCH_SIZE = 50  # Process tickers in batches
DELAY_BETWEEN_REQUESTS = 0.5  # Seconds between API calls
DELAY_BETWEEN_BATCHES = 10  # Seconds between batches
MAX_RETRIES = 3

class AmiBrokerDataUpdater:
    def __init__(self, data_folder: str = DATA_FOLDER, log_folder: str= LOG_FOLDER, ticker_file: str = TICKER_FILE):
        self.data_folder = data_folder
        self.log_folder = log_folder
        self.ticker_file = ticker_file
        self.ensure_data_folder()
        self.setup_logging()
        
    def ensure_data_folder(self):
        """Create data folder if it doesn't exist"""
        os.makedirs(self.data_folder, exist_ok=True)
        
    def setup_logging(self):
        """Setup logging to file and console"""
        log_file = os.path.join(self.log_folder, "eod_update.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_tickers_from_file(self) -> List[str]:
        """Load tickers from the ticker list file"""
        try:
            if not os.path.exists(self.ticker_file):
                self.logger.error(f"Ticker file not found: {self.ticker_file}")
                return []
                
            with open(self.ticker_file, 'r', encoding='utf-8') as f:
                tickers = [line.strip() for line in f if line.strip()]
                
            # Remove empty lines and duplicates
            tickers = [t for t in tickers if t]
            tickers = list(dict.fromkeys(tickers))  # Preserve order while removing duplicates
            
            self.logger.info(f"Loaded {len(tickers)} tickers from {self.ticker_file}")
            return tickers
            
        except Exception as e:
            self.logger.error(f"Error loading tickers from {self.ticker_file}: {e}")
            return []
    
    def validate_ticker(self, ticker: str) -> bool:
        """Basic ticker validation"""
        if not ticker or len(ticker) > 10:
            return False
        # Remove any common suffixes that might cause issues
        invalid_suffixes = ['.', '/', '\\', ':', '*', '?', '"', '<', '>', '|']
        return not any(suffix in ticker for suffix in invalid_suffixes)
    
    def get_latest_date_from_file(self, ticker: str) -> Optional[datetime]:
        """Get the latest date from existing ticker file"""
        file_path = os.path.join(self.data_folder, f"{ticker}.txt")
        
        if not os.path.exists(file_path):
            return None
            
        try:
            with open(file_path, 'r', encoding='ascii') as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
            
            if not lines:
                return None
                
            last_line = lines[-1]
            parts = last_line.split(',')
            if len(parts) < 2:
                return None
                
            last_date_str = parts[1]
            return datetime.strptime(last_date_str, '%Y-%m-%d')
            
        except Exception as e:
            self.logger.warning(f"Error reading {ticker}.txt: {e}")
            return None
    
    def download_ticker_data_with_retry(self, ticker: str, start_date: Optional[datetime] = None) -> Tuple[Optional[pd.DataFrame], int]:
        """Download data for ticker with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                if start_date:
                    # Add buffer to ensure we get all data
                    start_date_buffer = start_date - timedelta(days=7)
                    data = yf.download(
                        ticker, 
                        start=start_date_buffer, 
                        end=datetime.now() + timedelta(days=1),
                        auto_adjust=True, 
                        progress=False,
                        timeout=30
                    )
                    # Filter to only keep data from start_date onward
                    if not data.empty:
                        data = data[data.index >= pd.Timestamp(start_date)]
                else:
                    data = yf.download(
                        ticker, 
                        period="max", 
                        auto_adjust=True, 
                        progress=False,
                        timeout=30
                    )
                
                if data.empty:
                    return None, attempt + 1
                    
                return data, attempt + 1
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (attempt + 1) * 5  # Exponential backoff
                    self.logger.debug(f"Retry {attempt + 1} for {ticker} after {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to download {ticker} after {MAX_RETRIES} attempts: {e}")
                    return None, MAX_RETRIES
        return None, MAX_RETRIES
    
    def format_data_for_amibroker(self, ticker: str, data: pd.DataFrame) -> List[str]:
        """Convert DataFrame to AmiBroker ASCII format"""
        lines = []
        for date in data.index:
            row = data.loc[date]
            try:
                # Safe value extraction
                open_val = float(row['Open'].iloc[0]) if hasattr(row['Open'], 'iloc') else float(row['Open'])
                high_val = float(row['High'].iloc[0]) if hasattr(row['High'], 'iloc') else float(row['High'])
                low_val = float(row['Low'].iloc[0]) if hasattr(row['Low'], 'iloc') else float(row['Low'])
                close_val = float(row['Close'].iloc[0]) if hasattr(row['Close'], 'iloc') else float(row['Close'])
                volume_val = int(row['Volume'].iloc[0]) if hasattr(row['Volume'], 'iloc') else int(row['Volume'])
                
                date_str = date.strftime('%Y-%m-%d')
                line = f"{ticker},{date_str},{open_val:.6f},{high_val:.6f},{low_val:.6f},{close_val:.6f},{volume_val}"
                lines.append(line)
            except Exception as e:
                self.logger.debug(f"Skipping {date} for {ticker}: {e}")
                continue
        return lines
    
    def update_ticker(self, ticker: str) -> Tuple[bool, int, int]:
        """Update data for a single ticker"""
        try:
            # Validate ticker format
            if not self.validate_ticker(ticker):
                self.logger.warning(f"Invalid ticker format: {ticker}")
                return False, 0, 0
                
            latest_date = self.get_latest_date_from_file(ticker)
            file_path = os.path.join(self.data_folder, f"{ticker}.txt")
            
            if latest_date:
                # Incremental update
                if latest_date.date() >= datetime.now().date():
                    return True, 0, 1  # Already up to date
                    
                new_data, retries = self.download_ticker_data_with_retry(ticker, latest_date + timedelta(days=1))
                
                if new_data is None or new_data.empty:
                    return True, 0, retries  # No new data
                    
                new_lines = self.format_data_for_amibroker(ticker, new_data)
                
                with open(file_path, 'a', encoding='ascii') as f:
                    f.write('\n' + '\n'.join(new_lines))
                    
                return True, len(new_lines), retries
                
            else:
                # Full download
                full_data, retries = self.download_ticker_data_with_retry(ticker)
                
                if full_data is None or full_data.empty:
                    return False, 0, retries
                    
                all_lines = self.format_data_for_amibroker(ticker, full_data)
                
                with open(file_path, 'w', encoding='ascii') as f:
                    f.write('\n'.join(all_lines))
                    
                return True, len(all_lines), retries
                
        except Exception as e:
            self.logger.error(f"Unexpected error processing {ticker}: {e}")
            return False, 0, MAX_RETRIES
    
    def process_ticker_batch(self, tickers: List[str], batch_num: int) -> Tuple[int, int, int]:
        """Process a batch of tickers"""
        self.logger.info(f"Processing batch {batch_num} ({len(tickers)} tickers)...")
        
        successful = 0
        total_records = 0
        total_retries = 0
        
        for i, ticker in enumerate(tickers, 1):
            try:
                success, records_added, retries = self.update_ticker(ticker)
                total_retries += retries
                
                if success:
                    successful += 1
                    total_records += records_added
                    
                    # Minimal output - only show action
                    if records_added > 0:
                        print(f"{ticker}: +{records_added} records")
                    # else: (no output for already up-to-date tickers)
                else:
                    print(f"{ticker}: FAILED")
                    
                # Rate limiting between requests
                if i < len(tickers):
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    
            except Exception as e:
                self.logger.error(f"Batch processing error for {ticker}: {e}")
                print(f"{ticker}: ERROR")
                
        return successful, total_records, total_retries
    
    def update_all_tickers(self):
        """Update all tickers from the ticker file"""
        # Load tickers from file
        tickers = self.load_tickers_from_file()
        if not tickers:
            print("❌ No tickers loaded from file. Please check the ticker file path.")
            return
        
        start_time = time.time()
        self.logger.info("=" * 60)
        self.logger.info("Starting EoD data update")
        self.logger.info(f"Total tickers: {len(tickers)}")
        
        print(f"🚀 Updating {len(tickers)} tickers from {self.ticker_file}")
        print("=" * 50)
        
        # Split tickers into batches
        batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
        
        total_successful = 0
        total_records = 0
        total_retries = 0
        failed_tickers = []
        
        for batch_num, batch_tickers in enumerate(batches, 1):
            successful, records, retries = self.process_ticker_batch(batch_tickers, batch_num)
            total_successful += successful
            total_records += records
            total_retries += retries
            
            # Track failed tickers by checking which ones still don't have files
            for ticker in batch_tickers:
                if self.get_latest_date_from_file(ticker) is None:
                    failed_tickers.append(ticker)
            
            # Progress update
            progress = (batch_num / len(batches)) * 100
            print(f"Progress: {progress:.1f}% ({batch_num}/{len(batches)} batches)")
            
            # Rate limiting between batches
            if batch_num < len(batches):
                self.logger.info(f"Waiting {DELAY_BETWEEN_BATCHES}s before next batch...")
                time.sleep(DELAY_BETWEEN_BATCHES)
        
        # Calculate statistics
        duration = time.time() - start_time
        success_rate = (total_successful / len(tickers)) * 100
        
        # Final summary
        print("\n" + "=" * 50)
        print("📊 UPDATE COMPLETE")
        print(f"✅ Successful: {total_successful}/{len(tickers)} ({success_rate:.1f}%)")
        print(f"📈 Records added: {total_records:,}")
        print(f"🔄 Total retries: {total_retries}")
        print(f"⏱️  Duration: {duration:.1f}s")
        print(f"💾 Data location: {self.data_folder}")
        print(f"📋 Ticker source: {self.ticker_file}")
        
        if failed_tickers:
            print(f"\n❌ Failed tickers ({len(failed_tickers)}):")
            # Show first 10 failed tickers
            for ticker in failed_tickers[:10]:
                print(f"  {ticker}")
            if len(failed_tickers) > 10:
                print(f"  ... and {len(failed_tickers) - 10} more")
            
            # Save failed tickers to file for retry
            failed_file = os.path.join(self.data_folder, "failed_tickers.txt")
            with open(failed_file, 'w') as f:
                for ticker in failed_tickers:
                    f.write(f"{ticker}\n")
            print(f"📝 Failed tickers saved to: {failed_file}")
        
        self.logger.info(f"Update completed: {total_successful}/{len(tickers)} successful, {total_records} records added")
        self.logger.info(f"Duration: {duration:.1f}s, Retries: {total_retries}")

def main():
    updater = AmiBrokerDataUpdater()
    updater.update_all_tickers()

if __name__ == "__main__":
    main()