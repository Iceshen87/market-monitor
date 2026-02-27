#!/usr/bin/env python3
"""Tests for data aggregation pipeline."""
import asyncio
import tempfile
import json
from pathlib import Path
import data_aggregator as da

async def test_rest_fetcher():
    config = da.SourceConfig(
        name="test_api",
        source_type="rest",
        url="https://api.python.org/pep/2/json",
        timeout=5.0
    )
    fetcher = da.RESTFetcher(config)
    
    async with aiohttp.ClientSession() as session:
        records = await fetcher.fetch(session)
    
    assert len(records) >= 1
    print("REST fetcher: OK")

async def test_csv_fetcher():
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        f.write(b"value,timestamp,unit\n1.0,2026-02-27,USD\n2.5,2026-02-28,USD\n")
        f.flush()
        
        config = da.SourceConfig(
            name="test_csv",
            source_type="csv",
            file_path=f.name
        )
        fetcher = da.CSVFetcher(config)
        records = await fetcher.fetch()
        
        assert len(records) == 2
        print("CSV fetcher: OK")

async def test_file_fetcher():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        json.dump([{"value": 100, "timestamp": "2026-02-27"}], f)
        f.flush()
        
        config = da.SourceConfig(
            name="test_file",
            source_type="file",
            file_path=f.name
        )
        fetcher = da.FileFetcher(config)
        records = await fetcher.fetch()
        
        assert len(records) == 1
        assert records[0].value == 100.0
        print("File fetcher: OK")

async def test_aggregator():
    agg = da.DataAggregator()
    
    agg.add_source(da.SourceConfig(
        name="test",
        source_type="rest",
        url="https://api.python.org/pep/2/json"
    ))
    
    result = await agg.run()
    
    assert "records" in result
    assert "stats" in result
    print("Aggregator: OK")

if __name__ == "__main__":
    asyncio.run(test_aggregator())
