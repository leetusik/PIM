"""
Moving averages calculation service - now uses consolidated stock_analysis
"""
from app.services.stock_analysis import calculate_moving_averages_batch_parallel

def populate_all_moving_averages(max_workers: int = 4, chunk_size=None):
    """Calculate and populate moving averages for all stocks using optimized batch processing"""
    return calculate_moving_averages_batch_parallel(
        max_workers=max_workers,
        chunk_size=chunk_size
    )


if __name__ == "__main__":
    # Configuration
    MAX_WORKERS = 4  # Adjust based on your system and database connection limits
    CHUNK_SIZE = None  # Process all stocks at once, or set to e.g., 50 for chunked processing
    
    # Run the optimized batch parallel processing
    results = calculate_moving_averages_batch_parallel(
        max_workers=MAX_WORKERS,
        chunk_size=CHUNK_SIZE
    )