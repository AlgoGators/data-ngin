import logging
import asyncio
from utils.dynamic_loader import load_config
from data.orchestrator import Orchestrator

"""
TO-DO:
- Some symbols from contract.csv aren't in CME dataset
- Remove checking for 'date' or reevaluate how DB gives raw data (might
use different column name for date)
"""

def main() -> None:
    """
    Main entry point for the data pipeline.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        # Load configuration
        config_path = "data/config/config.yaml"  # Path to the YAML configuration file
        logging.info(f"Loading configuration from {config_path}")
        config = load_config(config_path)

        # Initialize the Orchestrator
        logging.info("Initializing orchestrator...")
        orchestrator = Orchestrator(config=config)

        # Run the pipeline
        logging.info("Starting the data pipeline...")
        asyncio.run(orchestrator.run())

        logging.info("Pipeline execution completed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
