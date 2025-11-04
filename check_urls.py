#!/usr/bin/env python3
"""
ULTRA-FAST parallel URL checker with aggressive optimizations
"""

import asyncio
import aiohttp
from pathlib import Path
from typing import List
import sys
from queue import Queue
from threading import Thread
import time


class URLChecker:
    def __init__(self, input_file: str, output_file: str, max_concurrent: int = 1000):
        self.input_file = input_file
        self.output_file = output_file
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.checked_count = 0
        self.working_count = 0
        self.working_urls = []
        self.lock = asyncio.Lock()
        self.start_time = None
        
    async def check_url(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Check if URL/products.json is accessible - OPTIMIZED"""
        url = url.rstrip('/')
        check_url = f"{url}/products.json"
        
        async with self.semaphore:
            try:
                async with session.get(check_url, allow_redirects=True) as response:
                    if response.status == 200:
                        # Read just enough to verify content exists
                        chunk = await response.content.read(50)
                        if chunk and len(chunk) > 0:
                            return True
            except:
                pass
            
            return False
    
    async def process_url(self, session: aiohttp.ClientSession, url: str):
        """Process a single URL"""
        if await self.check_url(session, url):
            async with self.lock:
                self.working_urls.append(url)
                self.working_count += 1
        
        async with self.lock:
            self.checked_count += 1
            if self.checked_count % 500 == 0:
                elapsed = time.time() - self.start_time
                rate = self.checked_count / elapsed if elapsed > 0 else 0
                print(f"Checked: {self.checked_count} | Working: {self.working_count} | Rate: {rate:.1f} URLs/sec", flush=True)
    
    async def batch_write(self):
        """Periodically write accumulated URLs to file"""
        while True:
            await asyncio.sleep(2)  # Write every 2 seconds
            async with self.lock:
                if self.working_urls:
                    with open(self.output_file, 'a') as f:
                        for url in self.working_urls:
                            f.write(f"{url}\n")
                    self.working_urls.clear()
    
    async def run(self):
        """Main execution function"""
        self.start_time = time.time()
        
        # Read all URLs
        print(f"Reading URLs from {self.input_file}...")
        with open(self.input_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        total_urls = len(urls)
        print(f"Found {total_urls} URLs to check")
        print(f"Starting ULTRA-FAST parallel check with {self.max_concurrent} concurrent connections...\n")
        
        # Configure session with aggressive settings
        connector = aiohttp.TCPConnector(
            limit=0,  # No limit on total connections
            limit_per_host=0,  # No limit per host
            ttl_dns_cache=600,
            force_close=False,
            enable_cleanup_closed=True
        )
        
        # Shorter timeout for speed
        timeout = aiohttp.ClientTimeout(total=5, connect=3)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0'},
            connector_owner=True
        ) as session:
            # Start batch writer
            writer_task = asyncio.create_task(self.batch_write())
            
            # Process all URLs in parallel
            tasks = [self.process_url(session, url) for url in urls]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Cancel writer and do final write
            writer_task.cancel()
            try:
                await writer_task
            except asyncio.CancelledError:
                pass
            
            # Final write
            if self.working_urls:
                with open(self.output_file, 'a') as f:
                    for url in self.working_urls:
                        f.write(f"{url}\n")
        
        elapsed = time.time() - self.start_time
        rate = self.checked_count / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"COMPLETE!")
        print(f"Total checked: {self.checked_count}")
        print(f"Working URLs: {self.working_count}")
        print(f"Time elapsed: {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} URLs/second")
        print(f"Saved to: {self.output_file}")
        print(f"{'='*60}")


async def main():
    checker = URLChecker(
        input_file='urls.txt',
        output_file='working_url.txt',
        max_concurrent=1000  # MUCH higher for speed
    )
    await checker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Partial results saved.")
        sys.exit(0)
