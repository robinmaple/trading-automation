"""
Standalone Scanner Entry Point
Dedicated scanner that uses EOD data and operates independently of trading components
"""

import argparse
import datetime
import sys
import os
from typing import List, Dict, Any

# Add the project root to Python path to allow absolute imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.scanner.integration import ibkr_data_adapter
from src.core.ibkr_client import IbkrClient
from src.data_feeds.ibkr_data_feed import IBKRDataFeed
from src.scanner.scan_manager import ScanManager
from src.scanner.integration.ibkr_data_adapter import IBKRDataAdapter
from config.scanner_config import ScannerConfig

# Context-aware logging imports
from src.core.context_aware_logger import (
    get_context_logger, 
    start_trading_session, 
    end_trading_session, 
    TradingEventType
)


def run_standalone_scanner(ibkr_client, data_feed, scanner_config=None) -> List[Dict[str, Any]]:
    """
    Standalone scanner execution - no trading components, only EOD data
    """
    context_logger = get_context_logger()
    
    try:
        # <Standalone Scanner Start Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Starting standalone scanner execution",
            context_provider={
                "data_type": "EOD/snapshot",
                "trading_components": "excluded",
                "scanner_config_provided": scanner_config is not None,
                "execution_mode": "standalone_eod_only"
            }
        )
        # <Standalone Scanner Start Logging - End>
        
        print("🔍 STANDALONE SCANNER ACTIVATED")
        print("=" * 50)
        print("📊 Mode: EOD Data Only")
        print("🚫 Trading Components: Excluded")
        print("💾 Output: Timestamped Excel Files")
        print("=" * 50)
        
        # Use provided config or create default
        if not scanner_config:
            scanner_config = ScannerConfig(
                enabled_strategies=['bull_trend_pullback'],
                min_confidence_score=60,
                max_candidates=25,
                use_eod_data=True
            )
            # <Default Config Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Using default scanner configuration",
                context_provider={
                    "default_strategies": ['bull_trend_pullback'],
                    "default_min_confidence": 60,
                    "default_max_candidates": 25,
                    "use_eod_data": True
                },
                decision_reason="No scanner configuration provided, using defaults"
            )
            # <Default Config Logging - End>
        else:
            # <Custom Config Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Using custom scanner configuration",
                context_provider={
                    "enabled_strategies": scanner_config.enabled_strategies,
                    "min_confidence_score": scanner_config.min_confidence_score,
                    "max_candidates": scanner_config.max_candidates,
                    "use_eod_data": scanner_config.use_eod_data
                },
                decision_reason="Using provided scanner configuration"
            )
            # <Custom Config Logging - End>
        
        # Initialize scanner components
        # <Component Initialization Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Initializing scanner components",
            context_provider={
                "components": ["IBKRDataAdapter", "ScanManager"],
                "data_adapter_type": "IBKRDataAdapter",
                "scan_manager_type": "ScanManager"
            }
        )
        # <Component Initialization Logging - End>
        
        data_adapter = IBKRDataAdapter(data_feed)
        scan_manager = ScanManager(data_adapter, scanner_config)

        # After creating ibkr_data_adapter, add:
        print("🔧 RUNNING HISTORICAL DATA TEST...")

        # Access the eod_provider from the adapter instance
        if hasattr(ibkr_data_adapter, 'eod_provider') and ibkr_data_adapter.eod_provider:
            test_result = data_adapter.eod_provider.test_historical_data_manual("AAPL")
            print(f"🔧 TEST RESULT: {'PASSED' if test_result else 'FAILED'}")
        else:
            print("❌ TEST: No eod_provider found in adapter")
        
        # <Components Ready Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner components initialized successfully",
            context_provider={
                "data_adapter_ready": data_adapter is not None,
                "scan_manager_ready": scan_manager is not None,
                "scanner_config_applied": True
            },
            decision_reason="Scanner components ready for candidate generation"
        )
        # <Components Ready Logging - End>
        
        # Generate candidates using EOD data
        print("\n🎯 Generating scanner candidates with EOD data...")
        
        # <Candidate Generation Start Logging - Begin>
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Starting candidate generation with EOD data",
            context_provider={
                "generation_method": "generate_all_candidates",
                "save_to_excel": True,
                "excel_output_dir": "scanner_results",
                "data_source": "historical_eod"
            }
        )
        # <Candidate Generation Start Logging - End>
        
        candidates = scan_manager.generate_all_candidates(save_to_excel=True, excel_output_dir="scanner_results")
        
        # <Scanner Results Logging - Begin>
        context_logger.log_event(
            TradingEventType.EXECUTION_DECISION,
            "Standalone scanner execution completed",
            context_provider={
                "candidates_found": len(candidates) if candidates else 0,
                "data_source": "EOD",
                "excel_output_generated": len(candidates) > 0,
                "execution_success": True,
                "candidate_breakdown": {
                    "total": len(candidates),
                    "sample_symbols": [c['symbol'] for c in candidates[:3]] if candidates else []
                }
            },
            decision_reason=f"Scanner completed with {len(candidates) if candidates else 0} candidates using EOD data"
        )
        # <Scanner Results Logging - End>
        
        if candidates:
            print(f"✅ Found {len(candidates)} candidates meeting criteria")
            
            # <Candidates Found Logging - Begin>
            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Candidates successfully generated",
                context_provider={
                    "candidate_count": len(candidates),
                    "confidence_scores": [c.get('confidence_score', 0) for c in candidates[:5]],
                    "strategy_breakdown": {
                        c.get('identified_by', 'unknown'): sum(1 for cand in candidates if cand.get('identified_by') == c.get('identified_by'))
                        for c in candidates[:10]
                    }
                },
                decision_reason=f"Successfully generated {len(candidates)} candidates meeting scanner criteria"
            )
            # <Candidates Found Logging - End>
            
            # Display results
            print("\n📊 SCANNER RESULTS:")
            print("-" * 40)
            for i, candidate in enumerate(candidates[:15]):  # Show top 15
                strategy = candidate.get('identified_by', 'basic_screening')
                confidence = candidate.get('confidence_score', 'N/A')
                print(f"   {i+1:2d}. {candidate['symbol']:6} - ${candidate['current_price']:7.2f} - {strategy} - {confidence}%")
            
            if len(candidates) > 15:
                print(f"   ... and {len(candidates) - 15} more candidates")
                
        else:
            # <No Candidates Logging - Begin>
            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "No candidates found meeting criteria",
                context_provider={
                    "candidate_count": 0,
                    "possible_reasons": [
                        "min_confidence_score too high",
                        "strategy criteria too strict", 
                        "market conditions unfavorable",
                        "data retrieval issues"
                    ]
                },
                decision_reason="Scanner completed but no candidates met the current criteria"
            )
            # <No Candidates Logging - End>
            print("⚠️  No candidates found meeting the current criteria")
            print("💡 Try adjusting min_confidence_score or strategy parameters")
            
        return candidates if candidates else []
            
    except Exception as e:
        print(f"❌ Standalone scanner failed: {e}")
        import traceback
        traceback.print_exc()
        
        # <Scanner Error Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Standalone scanner execution failed",
            context_provider={
                "error_type": type(e).__name__,
                "error_message": str(e),
                "execution_phase": "candidate_generation",
                "recovery_possible": False
            },
            decision_reason=f"Scanner execution terminated with error: {e}"
        )
        # <Scanner Error Logging - End>
        
        return []


def main():
    """
    Main entry point for standalone scanner
    """
    session_file = None
    ibkr_client = None
    
    # Initialize context-aware logger
    context_logger = get_context_logger()

    try:
        # Start scanner session
        session_file = start_trading_session()
        
        # <Scanner Session Start Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Standalone scanner session starting",
            context_provider={
                "session_file": session_file,
                "start_time": datetime.datetime.now().isoformat(),
                "component": "standalone_scanner",
                "session_type": "scanner_only"
            }
        )
        # <Scanner Session Start Logging - End>
        
        print(f"📝 Scanner session started: {session_file}")
        print(f"⏰ Session time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        parser = argparse.ArgumentParser(description="Standalone Stock Scanner (EOD Data)")
        parser.add_argument(
            "--mode",
            choices=["live", "paper"],
            default="paper",
            help="IBKR connection mode: live (port 7496) or paper (port 7497). Default: paper"
        )
        parser.add_argument(
            "--strategies",
            nargs="+",
            default=["bull_trend_pullback"],
            help="Scanner strategies to enable. Default: bull_trend_pullback"
        )
        parser.add_argument(
            "--min-confidence",
            type=float,
            default=60.0,
            help="Minimum confidence score (0-100). Default: 60.0"
        )
        parser.add_argument(
            "--max-candidates", 
            type=int,
            default=25,
            help="Maximum number of candidates to return. Default: 25"
        )
        
        args = parser.parse_args()

        # <Scanner Arguments Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Standalone scanner arguments parsed",
            context_provider={
                "mode": args.mode,
                "strategies": args.strategies,
                "min_confidence": args.min_confidence,
                "max_candidates": args.max_candidates,
                "argument_count": len(vars(args))
            },
            decision_reason="Command line arguments processed successfully"
        )
        # <Scanner Arguments Logging - End>

        # Display scanner configuration
        print("\n⚙️  SCANNER CONFIGURATION:")
        print(f"   Mode: {args.mode.upper()}")
        print(f"   Strategies: {', '.join(args.strategies)}")
        print(f"   Min Confidence: {args.min_confidence}%")
        print(f"   Max Candidates: {args.max_candidates}")
        print(f"   Data Type: EOD/Snapshot")
        print()

        # <Configuration Display Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner configuration displayed",
            context_provider={
                "operational_mode": args.mode.upper(),
                "enabled_strategies_count": len(args.strategies),
                "min_confidence_setting": args.min_confidence,
                "max_candidates_setting": args.max_candidates
            }
        )
        # <Configuration Display Logging - End>

        # Setup IBKR connection for data access
        ibkr_client = IbkrClient()
        port = 7496 if args.mode == "live" else 7497
        
        print(f"🔌 Connecting to IBKR API at 127.0.0.1:{port} ({args.mode.upper()} mode)...")
        
        # <IBKR Connection Attempt Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Attempting IBKR connection for scanner data",
            context_provider={
                "host": "127.0.0.1",
                "port": port,
                "mode": args.mode,
                "purpose": "scanner_eod_data",
                "client_type": "IbkrClient"
            }
        )
        # <IBKR Connection Attempt Logging - End>

        if not ibkr_client.connect("127.0.0.1", port, 0):
            print("❌ Failed to connect to IBKR")
            print("💡 Ensure IB Gateway/TWS is running and API connections are enabled")
            
            # <IBKR Connection Failure Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "IBKR connection failed for scanner",
                context_provider={
                    "host": "127.0.0.1",
                    "port": port,
                    "mode": args.mode,
                    "connection_attempted": True,
                    "connection_success": False
                },
                decision_reason="Scanner cannot proceed without data connection"
            )
            # <IBKR Connection Failure Logging - End>
            
            return

        # <IBKR Connection Success Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "IBKR connection established for scanner",
            context_provider={
                "host": "127.0.0.1",
                "port": port,
                "mode": args.mode,
                "connected": ibkr_client.connected,
                "connection_method": "direct_api"
            },
            decision_reason="IBKR connection successful, proceeding with scanner setup"
        )
        # <IBKR Connection Success Logging - End>

        # Create data feed (minimal - only for EOD data access)
        data_feed = IBKRDataFeed(ibkr_client, event_bus=None)
        
        # <Data Feed Initialization Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Data feed initialized for EOD data access",
            context_provider={
                "data_feed_type": "IBKRDataFeed",
                "event_bus_used": False,
                "purpose": "eod_data_only"
            }
        )
        # <Data Feed Initialization Logging - End>
        
        print("✅ Data feed initialized for EOD data access")

        # Configure scanner
        scanner_config = ScannerConfig(
            enabled_strategies=args.strategies,
            min_confidence_score=args.min_confidence,
            max_candidates=args.max_candidates,
            use_eod_data=True
        )

        # <Scanner Configuration Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Scanner configuration applied",
            context_provider={
                "config_applied": True,
                "strategies_enabled": scanner_config.enabled_strategies,
                "min_confidence": scanner_config.min_confidence_score,
                "max_candidates": scanner_config.max_candidates,
                "use_eod_data": scanner_config.use_eod_data
            },
            decision_reason="Scanner configuration finalized and ready for execution"
        )
        # <Scanner Configuration Logging - End>

        # Run standalone scanner
        candidates = run_standalone_scanner(ibkr_client, data_feed, scanner_config)
        
        # Final summary
        print("\n" + "=" * 50)
        if candidates:
            # <Scan Complete Success Logging - Begin>
            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Scanner execution completed successfully",
                context_provider={
                    "candidates_found": len(candidates),
                    "execution_result": "success",
                    "output_generated": True,
                    "output_location": "scanner_results/",
                    "session_file": session_file
                },
                decision_reason=f"Scanner completed with {len(candidates)} candidates, results saved to Excel"
            )
            # <Scan Complete Success Logging - End>
            print(f"🎯 SCAN COMPLETE: {len(candidates)} candidates found")
            print("💾 Results saved to timestamped Excel file in 'scanner_results/' folder")
        else:
            # <Scan Complete No Candidates Logging - Begin>
            context_logger.log_event(
                TradingEventType.EXECUTION_DECISION,
                "Scanner execution completed with no candidates",
                context_provider={
                    "candidates_found": 0,
                    "execution_result": "success_no_candidates",
                    "output_generated": False,
                    "session_file": session_file
                },
                decision_reason="Scanner completed but no candidates met the criteria"
            )
            # <Scan Complete No Candidates Logging - End>
            print("🎯 SCAN COMPLETE: No candidates found")
        print("=" * 50)

    except Exception as e:
        print(f"❌ Fatal error in standalone scanner: {e}")
        import traceback
        traceback.print_exc()
        
        # <Scanner Fatal Error Logging - Begin>
        context_logger.log_event(
            TradingEventType.SYSTEM_HEALTH,
            "Fatal error in standalone scanner main execution",
            context_provider={
                "error_type": type(e).__name__,
                "error_message": str(e),
                "session_file": session_file,
                "execution_phase": "main_scanner_loop",
                "recovery_attempted": False
            },
            decision_reason=f"Standalone scanner terminated with fatal error: {e}"
        )
        # <Scanner Fatal Error Logging - End>

    finally:
        print("\n🧹 Cleaning up scanner resources...")
        
        try:
            # End scanner session
            if session_file:
                end_trading_session()
                print(f"📁 Scanner session log saved: {session_file}")
            
            # Disconnect IBKR client
            if ibkr_client:
                ibkr_client.disconnect()
                print("✅ IBKR client disconnected")
                
            print("✅ Scanner cleanup completed")
            
            # <Scanner Cleanup Complete Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Standalone scanner cleanup completed",
                context_provider={
                    "session_ended": session_file is not None,
                    "ibkr_disconnected": ibkr_client is not None,
                    "system_state": "SHUTDOWN",
                    "cleanup_success": True
                },
                decision_reason="Scanner resources cleaned up successfully"
            )
            # <Scanner Cleanup Complete Logging - End>
            
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")
            
            # <Scanner Cleanup Warning Logging - Begin>
            context_logger.log_event(
                TradingEventType.SYSTEM_HEALTH,
                "Warning during scanner cleanup",
                context_provider={
                    "error": str(e),
                    "session_ended": session_file is not None,
                    "ibkr_disconnected": ibkr_client is not None,
                    "cleanup_success": False
                },
                decision_reason=f"Cleanup completed with warnings: {e}"
            )
            # <Scanner Cleanup Warning Logging - End>

if __name__ == "__main__":
    main()