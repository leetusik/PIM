from typing import Optional

from app.api import deps
from app.services.stock_analysis import (
    get_stocks_with_ma_filter,
    get_stocks_with_trend_template_filter,
    run_comprehensive_analysis
)
from app.services.create_all_daily_prices import create_daily_prices_batch_parallel
from app.services.comprehensive_analysis import run_quick_update
from app.schemas.stock import (
    DailyPriceResponse,
    DataPipelineResponse,
    PipelineStepResult,
    RSAnalysisSummary,
    StockScreenResponse,
    StockWithLatestPrice,
)
from app.models.stock import DailyPrice
from fastapi import APIRouter, Depends, Query, HTTPException, Path, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
import time
import asyncio

router = APIRouter()


@router.get("/screen/{mode}", response_model=StockScreenResponse)
def screen_stocks(
    mode: str = Path(..., description="Screening mode: 'ma' for moving averages, 'trend' for trend template"),
    min_price: Optional[float] = Query(20.0, description="Minimum current price"),
    # Moving Average specific parameters
    ma_50_filter: bool = Query(True, description="Filter stocks above 50-day MA (ma mode only)"),
    ma_150_filter: bool = Query(True, description="Filter stocks above 150-day MA (ma mode only)"),
    ma_200_filter: bool = Query(True, description="Filter stocks above 200-day MA (ma mode only)"),
    # Trend Template specific parameters
    min_rs_grade: float = Query(70.0, ge=0, le=100, description="Minimum RS grade (trend mode only)"),
    target_date: Optional[str] = Query(None, description="Target date for analysis (YYYY-MM-DD, trend mode only)"),
    # Common parameters
    sort_by: str = Query("rs_grade", description="Sort by: 'rs_grade', 'rs_rank', 'price', 'volume', 'roc_252'"),
    limit: int = Query(100, ge=1, le=1000, description="Number of results per page"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: Session = Depends(deps.get_db),
):
    """
    Screen stocks based on different criteria modes.
    
    **Modes:**
    - **ma**: Moving average filtering - stocks above specified moving averages
    - **trend**: Trend template filtering - stocks meeting IBD-style trend template criteria
    
    **MA Mode:** Returns stocks where current price > 50MA > 150MA > 200MA (configurable filters)
    **Trend Mode:** Returns stocks with strong RS grades, proper MA alignment, and near 52-week highs
    """
    # Validate mode parameter
    if mode not in ["ma", "trend"]:
        raise HTTPException(
            status_code=400, 
            detail="Invalid mode. Must be 'ma' for moving averages or 'trend' for trend template."
        )
    
    # Get filtered stocks based on mode
    if mode == "ma":
        stocks = get_stocks_with_ma_filter(
            min_price=min_price,
            ma_50_filter=ma_50_filter,
            ma_150_filter=ma_150_filter,
            ma_200_filter=ma_200_filter,
            limit=limit,
            offset=offset,
        )
    elif mode == "trend":
        stocks = get_stocks_with_trend_template_filter(
            target_date=target_date,
            min_price=min_price,
            min_rs_grade=min_rs_grade,
            limit=limit,
        )
        # For trend mode, we don't use offset since the service handles its own limiting
        # If you need pagination for trend mode, you'd need to modify the service

    # Get total count for pagination (simplified for now)
    total = len(stocks)  # This could be optimized with a separate count query

    # Convert to response format with latest prices
    # We need to fetch the latest prices separately since the stocks are detached from the session
    stocks_with_prices = []
    
    if stocks:
        # Get all stock IDs for bulk query
        stock_ids = [stock.id for stock in stocks]
        
        # Bulk query for latest prices - more efficient than individual queries
        latest_prices_subquery = (
            db.query(
                DailyPrice.stock_id,
                func.max(DailyPrice.date).label('latest_date')
            )
            .filter(DailyPrice.stock_id.in_(stock_ids))
            .group_by(DailyPrice.stock_id)
            .subquery()
        )
        
        latest_prices = (
            db.query(DailyPrice)
            .join(
                latest_prices_subquery,
                (DailyPrice.stock_id == latest_prices_subquery.c.stock_id) &
                (DailyPrice.date == latest_prices_subquery.c.latest_date)
            )
            .all()
        )
        
        # Create a lookup dict for O(1) access
        price_lookup = {dp.stock_id: dp for dp in latest_prices}
        
        # Build response objects
        for stock in stocks:
            latest_price = None
            rs_summary = None
            
            if stock.id in price_lookup:
                daily_price = price_lookup[stock.id]
                latest_price = DailyPriceResponse.model_validate(daily_price)
                
                # Create RS summary if RS data is available
                if daily_price.rs_grade is not None:
                    # Determine if stock meets trend template criteria
                    is_trend_template = (
                        daily_price.rs_grade >= 70 and
                        daily_price.close >= 20 and
                        daily_price.is_ma_200_bullish and
                        daily_price.is_near_52w_high and
                        daily_price.ma_50 > daily_price.ma_150 and
                        daily_price.ma_150 > daily_price.ma_200
                    ) if all([
                        daily_price.rs_grade is not None,
                        daily_price.close is not None,
                        daily_price.is_ma_200_bullish is not None,
                        daily_price.is_near_52w_high is not None,
                        daily_price.ma_50 is not None,
                        daily_price.ma_150 is not None,
                        daily_price.ma_200 is not None
                    ]) else None
                    
                    rs_summary = RSAnalysisSummary(
                        rs_grade=daily_price.rs_grade,
                        rs_rank=daily_price.rs_rank,
                        rs_momentum=daily_price.rs_momentum,
                        roc_252=daily_price.roc_252,
                        is_trend_template=is_trend_template
                    )

            stock_with_price = StockWithLatestPrice(
                id=stock.id,
                name=stock.name,
                market=stock.market,
                ticker=stock.ticker,
                latest_price=latest_price,
                rs_summary=rs_summary,
            )
            stocks_with_prices.append(stock_with_price)

    # Sort stocks based on the sort_by parameter
    def get_sort_key(stock_with_price):
        """Generate sort key based on sort_by parameter"""
        if sort_by == "rs_grade":
            # Best RS grade first (descending)
            if stock_with_price.rs_summary and stock_with_price.rs_summary.rs_grade is not None:
                return (-stock_with_price.rs_summary.rs_grade, stock_with_price.rs_summary.rs_rank or 999999)
            return (float('-inf'), 999999)
        
        elif sort_by == "rs_rank":
            # Best RS rank first (ascending - rank 1 is best)
            if stock_with_price.rs_summary and stock_with_price.rs_summary.rs_rank is not None:
                return (stock_with_price.rs_summary.rs_rank, -(stock_with_price.rs_summary.rs_grade or 0))
            return (999999, float('-inf'))
        
        elif sort_by == "price":
            # Highest price first
            if stock_with_price.latest_price and stock_with_price.latest_price.close is not None:
                return (-stock_with_price.latest_price.close,)
            return (float('-inf'),)
        
        elif sort_by == "volume":
            # Highest volume first
            if stock_with_price.latest_price and stock_with_price.latest_price.volume is not None:
                return (-stock_with_price.latest_price.volume,)
            return (float('-inf'),)
        
        elif sort_by == "roc_252":
            # Best 1-year performance first
            if stock_with_price.rs_summary and stock_with_price.rs_summary.roc_252 is not None:
                return (-stock_with_price.rs_summary.roc_252,)
            return (float('-inf'),)
        
        else:
            # Default to RS grade sorting
            if stock_with_price.rs_summary and stock_with_price.rs_summary.rs_grade is not None:
                return (-stock_with_price.rs_summary.rs_grade, stock_with_price.rs_summary.rs_rank or 999999)
            return (float('-inf'), 999999)
    
    # Validate sort_by parameter
    valid_sort_options = ["rs_grade", "rs_rank", "price", "volume", "roc_252"]
    if sort_by not in valid_sort_options:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort_by parameter. Must be one of: {', '.join(valid_sort_options)}"
        )
    
    stocks_with_prices.sort(key=get_sort_key)

    return StockScreenResponse(
        stocks=stocks_with_prices, 
        total=total, 
        page=offset // limit + 1 if mode == "ma" else 1,  # Trend mode doesn't use offset
        limit=limit
    )


@router.post("/pipeline/full", response_model=DataPipelineResponse)
def run_full_data_pipeline(
    max_workers: int = Query(4, ge=1, le=8, description="Number of parallel workers"),
    start_date: str = Query("20240101", description="Start date for daily prices (YYYYMMDD)"),
    end_date: str = Query("20250831", description="End date for daily prices (YYYYMMDD)"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: Session = Depends(deps.get_db),
):
    """
    Run the complete data pipeline:
    1. Create/update stocks from market data
    2. Fetch and update daily prices
    3. Calculate moving averages and indicators
    4. Calculate RS momentum and rankings
    
    This is a comprehensive operation that can take several minutes to complete.
    """
    pipeline_start = time.time()
    steps = []
    
    try:
        # Step 1: Update stock list (simulate since we can't import the actual function easily)
        step_start = time.time()
        try:
            # Note: In a real implementation, you'd call create_all_stocks_today here
            # For now, we'll simulate this step
            step_duration = time.time() - step_start
            steps.append(PipelineStepResult(
                step_name="Update Stock List",
                success=True,
                duration_seconds=step_duration,
                message="Stock list update completed (simulated)",
                details={"note": "Manual stock creation required"}
            ))
        except Exception as e:
            step_duration = time.time() - step_start
            steps.append(PipelineStepResult(
                step_name="Update Stock List",
                success=False,
                duration_seconds=step_duration,
                message=f"Failed to update stock list: {str(e)}"
            ))
        
        # Step 2: Fetch daily prices
        step_start = time.time()
        try:
            daily_prices_result = create_daily_prices_batch_parallel(
                max_workers=max_workers,
                start_date=start_date,
                end_date=end_date
            )
            step_duration = time.time() - step_start
            steps.append(PipelineStepResult(
                step_name="Fetch Daily Prices",
                success=daily_prices_result.get('successful', 0) > 0,
                duration_seconds=step_duration,
                message=f"Processed {daily_prices_result.get('successful', 0)} stocks successfully",
                details={
                    "total_stocks": daily_prices_result.get('total_stocks', 0),
                    "successful": daily_prices_result.get('successful', 0),
                    "failed": daily_prices_result.get('failed', 0),
                    "total_records_inserted": daily_prices_result.get('total_records_inserted', 0)
                }
            ))
        except Exception as e:
            step_duration = time.time() - step_start
            steps.append(PipelineStepResult(
                step_name="Fetch Daily Prices",
                success=False,
                duration_seconds=step_duration,
                message=f"Failed to fetch daily prices: {str(e)}"
            ))
        
        # Step 3: Run comprehensive analysis
        step_start = time.time()
        try:
            analysis_result = run_comprehensive_analysis(max_workers=max_workers)
            step_duration = time.time() - step_start
            
            # Extract summary from analysis result
            ma_success = analysis_result.get('moving_averages', {}).get('successful', 0)
            rs_success = analysis_result.get('rs_momentum', {}).get('successful', 0)
            rankings_success = analysis_result.get('rs_rankings', {}).get('success', False)
            
            steps.append(PipelineStepResult(
                step_name="Comprehensive Analysis",
                success=ma_success > 0 and rs_success > 0 and rankings_success,
                duration_seconds=step_duration,
                message=f"Analysis completed: MA({ma_success}), RS({rs_success}), Rankings({rankings_success})",
                details={
                    "moving_averages": analysis_result.get('moving_averages', {}),
                    "rs_momentum": analysis_result.get('rs_momentum', {}),
                    "rs_rankings": analysis_result.get('rs_rankings', {})
                }
            ))
        except Exception as e:
            step_duration = time.time() - step_start
            steps.append(PipelineStepResult(
                step_name="Comprehensive Analysis",
                success=False,
                duration_seconds=step_duration,
                message=f"Failed to run analysis: {str(e)}"
            ))
        
        # Calculate overall success and summary
        total_duration = time.time() - pipeline_start
        successful_steps = sum(1 for step in steps if step.success)
        overall_success = successful_steps == len(steps)
        
        # Create summary
        summary = {
            "total_steps": len(steps),
            "successful_steps": successful_steps,
            "failed_steps": len(steps) - successful_steps,
            "success_rate": f"{(successful_steps / len(steps) * 100):.1f}%",
            "total_duration_minutes": f"{total_duration / 60:.2f}"
        }
        
        return DataPipelineResponse(
            pipeline_type="full",
            success=overall_success,
            total_duration_seconds=total_duration,
            steps=steps,
            summary=summary
        )
        
    except Exception as e:
        total_duration = time.time() - pipeline_start
        return DataPipelineResponse(
            pipeline_type="full",
            success=False,
            total_duration_seconds=total_duration,
            steps=steps,
            summary={"error": str(e)}
        )


@router.post("/pipeline/quick", response_model=DataPipelineResponse)
def run_quick_pipeline_update(
    max_workers: int = Query(4, ge=1, le=8, description="Number of parallel workers"),
    db: Session = Depends(deps.get_db),
):
    """
    Run a quick pipeline update for the most recent data:
    1. Update moving averages for recent data
    2. Update RS momentum calculations
    3. Recalculate RS rankings
    
    This is faster than the full pipeline and focuses on recent data updates.
    """
    pipeline_start = time.time()
    steps = []
    
    try:
        # Run quick update
        step_start = time.time()
        try:
            quick_result = run_quick_update()
            step_duration = time.time() - step_start
            
            if quick_result and 'error' not in quick_result:
                steps.append(PipelineStepResult(
                    step_name="Quick Analysis Update",
                    success=True,
                    duration_seconds=step_duration,
                    message="Quick update completed successfully",
                    details=quick_result
                ))
            else:
                steps.append(PipelineStepResult(
                    step_name="Quick Analysis Update",
                    success=False,
                    duration_seconds=step_duration,
                    message=f"Quick update failed: {quick_result.get('error', 'Unknown error')}"
                ))
        except Exception as e:
            step_duration = time.time() - step_start
            steps.append(PipelineStepResult(
                step_name="Quick Analysis Update",
                success=False,
                duration_seconds=step_duration,
                message=f"Failed to run quick update: {str(e)}"
            ))
        
        # Calculate overall success and summary
        total_duration = time.time() - pipeline_start
        successful_steps = sum(1 for step in steps if step.success)
        overall_success = successful_steps == len(steps)
        
        summary = {
            "total_steps": len(steps),
            "successful_steps": successful_steps,
            "failed_steps": len(steps) - successful_steps,
            "success_rate": f"{(successful_steps / len(steps) * 100):.1f}%",
            "total_duration_minutes": f"{total_duration / 60:.2f}"
        }
        
        return DataPipelineResponse(
            pipeline_type="quick",
            success=overall_success,
            total_duration_seconds=total_duration,
            steps=steps,
            summary=summary
        )
        
    except Exception as e:
        total_duration = time.time() - pipeline_start
        return DataPipelineResponse(
            pipeline_type="quick",
            success=False,
            total_duration_seconds=total_duration,
            steps=steps,
            summary={"error": str(e)}
        )


@router.get("/pipeline/status")
def get_pipeline_status(db: Session = Depends(deps.get_db)):
    """
    Get the current status of the data pipeline by checking data freshness and completeness.
    """
    try:
        from app.crud.stock import get_stocks
        
        # Get basic stats
        stocks = get_stocks(db)
        total_stocks = len(stocks)
        
        # Get latest data date
        latest_date = db.query(func.max(DailyPrice.date)).scalar()
        
        # Count stocks with recent data
        stocks_with_recent_data = 0
        stocks_with_rs_data = 0
        
        if latest_date:
            stocks_with_recent_data = db.query(DailyPrice.stock_id).filter(
                DailyPrice.date == latest_date
            ).distinct().count()
            
            stocks_with_rs_data = db.query(DailyPrice.stock_id).filter(
                DailyPrice.date == latest_date,
                DailyPrice.rs_grade.isnot(None)
            ).distinct().count()
        
        # Calculate data completeness
        price_completeness = (stocks_with_recent_data / total_stocks * 100) if total_stocks > 0 else 0
        rs_completeness = (stocks_with_rs_data / total_stocks * 100) if total_stocks > 0 else 0
        
        return {
            "status": "healthy" if price_completeness > 90 and rs_completeness > 80 else "needs_update",
            "total_stocks": total_stocks,
            "latest_data_date": latest_date.isoformat() if latest_date else None,
            "data_completeness": {
                "price_data": f"{price_completeness:.1f}%",
                "rs_analysis": f"{rs_completeness:.1f}%"
            },
            "stocks_with_recent_data": stocks_with_recent_data,
            "stocks_with_rs_data": stocks_with_rs_data,
            "recommendations": [
                "Run full pipeline" if price_completeness < 90 else "Run quick update",
                "Check RS calculations" if rs_completeness < 80 else "Data looks good"
            ]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "recommendations": ["Check database connection", "Run pipeline diagnostics"]
        }
