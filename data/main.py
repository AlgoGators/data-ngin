import logging
import asyncio
from utils.dynamic_loader import load_config
from data.orchestrator import Orchestrator

def main() -> None:
    """
    Main entry point for the data pipeline.
    """
    # Step 1: Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        # Step 2: Load configuration
        config_path = "data/config/config.yaml"  # Path to the YAML configuration file
        logging.info(f"Loading configuration from {config_path}")
        config = load_config(config_path)

        # Step 3: Initialize the Orchestrator
        logging.info("Initializing orchestrator...")
        orchestrator = Orchestrator(config=config)

        # Step 4: Run the pipeline
        logging.info("Starting the data pipeline...")
        asyncio.run(orchestrator.run())

        logging.info("Pipeline execution completed successfully.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()
