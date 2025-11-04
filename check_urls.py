#!/usr/bin/env python3
"""
Fast parallel URL checker that verifies /products.json endpoints
"""

import asyncio
import aiohttp
from pathlib import Path
from typing import List
import sys


class URLChecker:
    def __init__(self, input_file: str, output_file: str, max_concurrent: int = 100):
        self.input_file = input_file
        self.output_file = output_file
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.checked_count = 0
        self.working_count = 0
        self.lock = asyncio.Lock()
        
    async def check_url(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Check if URL/products.json is accessible and has content"""
        # Remove trailing slash if present
        url = url.rstrip('/')
        check_url = f"{url}/products.json"
        
        async with self.semaphore:
            try:
                async with session.get(check_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        content = await response.text()
                        # Check if content is not empty
                        if content and len(content.strip()) > 0:
                            return True
            except Exception:
                # Silently ignore errors (timeout, connection errors, etc.)
                pass
            
            return False
    
    async def save_working_url(self, url: str):
        """Thread-safe append to output file"""
        async with self.lock:
            with open(self.output_file, 'a') as f:
                f.write(f"{url}\n")
            self.working_count += 1
    
    async def process_url(self, session: aiohttp.ClientSession, url: str):
        """Process a single URL"""
        if await self.check_url(session, url):
            await self.save_working_url(url)
        
        async with self.lock:
            self.checked_count += 1
            if self.checked_count % 100 == 0:
                print(f"Checked: {self.checked_count} | Working: {self.working_count}", flush=True)
    
    async def run(self):
        """Main execution function"""
        # Read all URLs
        print(f"Reading URLs from {self.input_file}...")
        with open(self.input_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        
        total_urls = len(urls)
        print(f"Found {total_urls} URLs to check")
        print(f"Starting parallel check with {self.max_concurrent} concurrent connections...\n")
        
        # Configure session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=10,
            ttl_dns_cache=300
        )
        
        timeout = aiohttp.ClientTimeout(total=10)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        ) as session:
            # Process all URLs in parallel
            tasks = [self.process_url(session, url) for url in urls]
            await asyncio.gather(*tasks)
        
        print(f"\n{'='*60}")
        print(f"COMPLETE!")
        print(f"Total checked: {self.checked_count}")
        print(f"Working URLs: {self.working_count}")
        print(f"Saved to: {self.output_file}")
        print(f"{'='*60}")


async def main():
    checker = URLChecker(
        input_file='urls.txt',
        output_file='working_url.txt',
        max_concurrent=100  # Adjust this for more/less parallelism
    )
    await checker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Partial results saved.")
        sys.exit(0)
