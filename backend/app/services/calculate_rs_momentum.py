"""
RS momentum calculation service - now uses consolidated stock_analysis
"""
from app.services.stock_analysis import calculate_rs_momentum_batch_parallel


def populate_all_rs_momentum(max_workers: int = 4, chunk_size=None):
    """Calculate and populate RS momentum for all stocks using optimized batch processing"""
    return calculate_rs_momentum_batch_parallel(
        max_workers=max_workers,
        chunk_size=chunk_size
    )


if __name__ == "__main__":
    populate_all_rs_momentum()
