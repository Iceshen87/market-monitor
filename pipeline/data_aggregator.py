#!/usr/bin/env python3
"""
Multi-Source Data Aggregation & Normalization Pipeline

Fetches data from multiple sources (REST, CSV, WebSocket, GraphQL, file),
normalizes to a common schema, and outputs sorted JSON with statistics.
"""

import asyncio
import json
import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, AsyncIterator
from dataclasses import dataclass, asdict
import aiohttp
import websockets


@dataclass
class NormalizedRecord:
    """Standard schema for all data sources."""
    source: str
    timestamp: str
    value: float
    unit: str
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


class SourceConfig:
    """Configuration for a data source."""
    def __init__(
        self,
        name: str,
        source_type: str,  # rest, csv, websocket, graphql, file
        url: Optional[str] = None,
        file_path: Optional[str] = None,
        query: Optional[str] = None,
        timeout: float = 10.0,
        **kwargs
    ):
        self.name = name
        self.source_type = source_type
        self.url = url
        self.file_path = file_path
        self.query = query
        self.timeout = timeout
        self.extra = kwargs


class RESTFetcher:
    """Fetch data from REST API."""
    
    def __init__(self, config: SourceConfig):
        self.config = config
    
    async def fetch(self, session: aiohttp.ClientSession) -> List[NormalizedRecord]:
        records = []
        try:
            async with session.get(
                self.config.url,
                timeout=aiohttp.ClientTimeout(total=self.config.timeout)
            ) as response:
                data = await response.json()
                
                # Handle different response formats
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    items = data.get("data", data.get("results", [data]))
                else:
                    items = [data]
                
                for item in items:
                    record = self._normalize(item)
                    if record:
                        records.append(record)
                        
        except asyncio.TimeoutError:
            print(f"[{self.config.name}] Timeout after {self.config.timeout}s")
        except Exception as e:
            print(f"[{self.config.name}] Error: {e}")
        
        return records
    
    def _normalize(self, item: Dict) -> Optional[NormalizedRecord]:
        """Normalize REST response to standard schema."""
        # Try common field names
        value = item.get("value") or item.get("price") or item.get("amount") or item.get("data")
        timestamp = item.get("timestamp") or item.get("time") or item.get("date")
        unit = item.get("unit") or item.get("currency") or "USD"
        
        if value is None:
            return None
        
        # Parse timestamp
        if timestamp:
            if isinstance(timestamp, (int, float)):
                ts = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            else:
                ts = str(timestamp)
        else:
            ts = datetime.now(tz=timezone.utc).isoformat()
        
        return NormalizedRecord(
            source=self.config.name,
            timestamp=ts,
            value=float(value),
            unit=str(unit),
            metadata={"raw": item}
        )


class CSVFetcher:
    """Fetch data from CSV file."""
    
    def __init__(self, config: SourceConfig):
        self.config = config
    
    async def fetch(self) -> List[NormalizedRecord]:
        records = []
        path = Path(self.config.file_path)
        
        if not path.exists():
            print(f"[{self.config.name}] File not found: {path}")
            return records
        
        try:
            with open(path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    record = self._normalize(row)
                    if record:
                        records.append(record)
        except Exception as e:
            print(f"[{self.config.name}] Error reading CSV: {e}")
        
        return records
    
    def _normalize(self, row: Dict) -> Optional[NormalizedRecord]:
        """Normalize CSV row to standard schema."""
        # Try common column names
        value = row.get("value") or row.get("price") or row.get("amount")
        timestamp = row.get("timestamp") or row.get("date") or row.get("time")
        unit = row.get("unit") or row.get("currency") or "USD"
        
        if value is None:
            return None
        
        # Parse timestamp
        if timestamp:
            ts = str(timestamp)
        else:
            ts = datetime.now(tz=timezone.utc).isoformat()
        
        return NormalizedRecord(
            source=self.config.name,
            timestamp=ts,
            value=float(value),
            unit=str(unit)
        )


class WebSocketFetcher:
    """Fetch data from WebSocket."""
    
    def __init__(self, config: SourceConfig):
        self.config = config
        self.max_messages = config.extra.get("max_messages", 10)
    
    async def fetch(self) -> List[NormalizedRecord]:
        records = []
        message_count = 0
        
        try:
            async with websockets.connect(
                self.config.url,
                ping_interval=20,
                ping_timeout=10
            ) as ws:
                # Send subscription message if provided
                if self.config.extra.get("subscribe"):
                    await ws.send(json.dumps(self.config.extra["subscribe"]))
                
                while message_count < self.max_messages:
                    try:
                        msg = await asyncio.wait_for(
                            ws.recv(),
                            timeout=self.config.timeout
                        )
                        data = json.loads(msg)
                        record = self._normalize(data)
                        if record:
                            records.append(record)
                        message_count += 1
                    except asyncio.TimeoutError:
                        break
                        
        except Exception as e:
            print(f"[{self.config.name}] WebSocket error: {e}")
        
        return records
    
    def _normalize(self, data: Dict) -> Optional[NormalizedRecord]:
        """Normalize WebSocket message to standard schema."""
        # Handle different message formats
        value = data.get("value") or data.get("price") or data.get("p") or data.get("data")
        timestamp = data.get("timestamp") or data.get("time") or data.get("T") or data.get("E")
        unit = data.get("unit") or data.get("currency") or "USD"
        
        if value is None:
            return None
        
        # Parse timestamp
        if timestamp:
            if isinstance(timestamp, (int, float)):
                # Could be seconds or milliseconds
                if timestamp > 1e12:
                    timestamp = timestamp / 1000
                ts = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            else:
                ts = str(timestamp)
        else:
            ts = datetime.now(tz=timezone.utc).isoformat()
        
        return NormalizedRecord(
            source=self.config.name,
            timestamp=ts,
            value=float(value),
            unit=str(unit)
        )


class GraphQLFetcher:
    """Fetch data from GraphQL API."""
    
    def __init__(self, config: SourceConfig):
        self.config = config
    
    async def fetch(self, session: aiohttp.ClientSession) -> List[NormalizedRecord]:
        records = []
        
        try:
            payload = {"query": self.config.query}
            headers = {"Content-Type": "application/json"}
            
            async with session.post(
                self.config.url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.config.timeout)
            ) as response:
                data = await response.json()
                
                # Handle GraphQL response structure
                items = data.get("data", {})
                if isinstance(items, dict):
                    # Flatten nested data
                    items = self._flatten(items)
                
                for item in items if isinstance(items, list) else [items]:
                    record = self._normalize(item)
                    if record:
                        records.append(record)
                        
        except Exception as e:
            print(f"[{self.config.name}] GraphQL error: {e}")
        
        return records
    
    def _flatten(self, data: Dict, prefix: str = "") -> List[Dict]:
        """Flatten nested GraphQL response."""
        items = []
        for key, value in data.items():
            if isinstance(value, list):
                items.extend(value)
            elif isinstance(value, dict):
                items.extend(self._flatten(value, f"{prefix}{key}_"))
            else:
                items.append(data)
                break
        return items
    
    def _normalize(self, item: Dict) -> Optional[NormalizedRecord]:
        """Normalize GraphQL response to standard schema."""
        value = item.get("value") or item.get("price") or item.get("amount")
        timestamp = item.get("timestamp") or item.get("createdAt") or item.get("date")
        unit = item.get("unit") or item.get("currency") or "USD"
        
        if value is None:
            return None
        
        if timestamp:
            ts = str(timestamp)
        else:
            ts = datetime.now(tz=timezone.utc).isoformat()
        
        return NormalizedRecord(
            source=self.config.name,
            timestamp=ts,
            value=float(value),
            unit=str(unit)
        )


class FileFetcher:
    """Fetch data from JSON/XML file."""
    
    def __init__(self, config: SourceConfig):
        self.config = config
    
    async def fetch(self) -> List[NormalizedRecord]:
        records = []
        path = Path(self.config.file_path)
        
        if not path.exists():
            print(f"[{self.config.name}] File not found: {path}")
            return records
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                if path.suffix == '.json':
                    data = json.load(f)
                else:
                    # Treat as text, parse line by line
                    data = [line.strip() for line in f if line.strip()]
            
            items = data if isinstance(data, list) else [data]
            
            for item in items:
                record = self._normalize(item)
                if record:
                    records.append(record)
                    
        except Exception as e:
            print(f"[{self.config.name}] Error reading file: {e}")
        
        return records
    
    def _normalize(self, item) -> Optional[NormalizedRecord]:
        """Normalize file content to standard schema."""
        if isinstance(item, dict):
            value = item.get("value") or item.get("price") or item.get("amount")
            timestamp = item.get("timestamp") or item.get("date")
            unit = item.get("unit") or "USD"
            
            if value is None:
                return None
            
            ts = str(timestamp) if timestamp else datetime.now(tz=timezone.utc).isoformat()
            
            return NormalizedRecord(
                source=self.config.name,
                timestamp=ts,
                value=float(value),
                unit=str(unit)
            )
        elif isinstance(item, (int, float)):
            return NormalizedRecord(
                source=self.config.name,
                timestamp=datetime.now(tz=timezone.utc).isoformat(),
                value=float(item),
                unit="units"
            )
        
        return None


class DataAggregator:
    """Main aggregator that orchestrates all data sources."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.sources: List[SourceConfig] = []
        self.fetchers = []
        
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, path: str):
        """Load source configurations from JSON file."""
        with open(path, 'r') as f:
            config = json.load(f)
        
        for src in config.get("sources", []):
            self.add_source(SourceConfig(**src))
    
    def add_source(self, config: SourceConfig):
        """Add a data source."""
        self.sources.append(config)
        
        if config.source_type == "rest":
            self.fetchers.append(("rest", RESTFetcher(config)))
        elif config.source_type == "csv":
            self.fetchers.append(("csv", CSVFetcher(config)))
        elif config.source_type == "websocket":
            self.fetchers.append(("websocket", WebSocketFetcher(config)))
        elif config.source_type == "graphql":
            self.fetchers.append(("graphql", GraphQLFetcher(config)))
        elif config.source_type == "file":
            self.fetchers.append(("file", FileFetcher(config)))
    
    async def fetch_all(self) -> List[NormalizedRecord]:
        """Fetch from all sources concurrently."""
        all_records = []
        tasks = []
        
        async with aiohttp.ClientSession() as session:
            for fetcher_type, fetcher in self.fetchers:
                if fetcher_type in ("rest", "graphql"):
                    tasks.append(fetcher.fetch(session))
                else:
                    tasks.append(fetcher.fetch())
            
            # Run all fetches concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    print(f"Fetch error: {result}")
                elif isinstance(result, list):
                    all_records.extend(result)
        
        return all_records
    
    def normalize_and_merge(
        self,
        records: List[NormalizedRecord],
        sort_by: str = "timestamp"
    ) -> List[Dict]:
        """Merge records and sort."""
        # Convert to dicts
        items = [r.to_dict() for r in records]
        
        # Sort
        reverse = sort_by == "timestamp"  # newest first for timestamps
        items.sort(key=lambda x: x.get(sort_by, ""), reverse=reverse)
        
        return items
    
    def generate_stats(self, records: List[NormalizedRecord]) -> Dict:
        """Generate statistics about the data."""
        if not records:
            return {"total": 0, "sources": {}}
        
        stats = {
            "total": len(records),
            "sources": {},
            "value_range": {
                "min": min(r.value for r in records),
                "max": max(r.value for r in records),
            },
            "units": list(set(r.unit for r in records)),
        }
        
        # Count by source
        for record in records:
            if record.source not in stats["sources"]:
                stats["sources"][record.source] = 0
            stats["sources"][record.source] += 1
        
        return stats
    
    async def run(
        self,
        output_path: Optional[str] = None,
        include_stats: bool = True
    ) -> Dict:
        """Run the complete pipeline."""
        start_time = time.time()
        
        print(f"[{datetime.now().isoformat()}] Starting data aggregation...")
        
        # Fetch all data
        records = await self.fetch_all()
        
        # Normalize and merge
        merged = self.normalize_and_merge(records)
        
        # Generate stats
        stats = self.generate_stats(records) if include_stats else {}
        stats["fetch_time_seconds"] = round(time.time() - start_time, 2)
        
        result = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "records": merged,
        }
        
        if include_stats:
            result["stats"] = stats
        
        # Save to file if specified
        if output_path:
            with open(output_path, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"Saved {len(merged)} records to {output_path}")
        
        return result


# Example configuration
EXAMPLE_CONFIG = {
    "sources": [
        {
            "name": "hyperliquid_api",
            "source_type": "rest",
            "url": "https://api.hyperliquid.xyz/info",
            "timeout": 10
        },
        {
            "name": "market_data_csv",
            "source_type": "csv",
            "file_path": "./data/market_data.csv"
        },
        {
            "name": "price_stream",
            "source_type": "websocket",
            "url": "wss://stream.example.com/prices",
            "max_messages": 10
        },
        {
            "name": "analytics_api",
            "source_type": "graphql",
            "url": "https://api.example.com/graphql",
            "query": "{ prices { value timestamp currency } }"
        },
        {
            "name": "local_data",
            "source_type": "file",
            "file_path": "./data/local.json"
        }
    ]
}


async def main():
    """Run the pipeline with example config."""
    # Create aggregator
    aggregator = DataAggregator()
    
    # Add some example sources
    aggregator.add_source(SourceConfig(
        name="coingecko_api",
        source_type="rest",
        url="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
        timeout=10
    ))
    
    # Run pipeline
    result = await aggregator.run(output_path="aggregated_data.json")
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Aggregation Complete")
    print(f"Total records: {len(result.get('records', []))}")
    if 'stats' in result:
        print(f"Fetch time: {result['stats'].get('fetch_time_seconds')}s")
        print(f"Sources: {result['stats'].get('sources')}")
    print(f"{'='*50}")


if __name__ == "__main__":
    asyncio.run(main())