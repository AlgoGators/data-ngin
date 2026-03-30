import logging
import asyncio
import os
from utils.dynamic_loader import load_config
from src.orchestrator import Orchestrator

async def run_pipelines():
    """Wrapper to handle multiple async orchestrator runs safely."""
    # Ensure this matches where your .yaml files actually live
    config_dir = "src/config" 
    
    if not os.path.exists(config_dir):
        logging.error(f"Configuration directory not found: {config_dir}")
        return

    # Filter for yaml files first to keep the log clean
    config_files = [f for f in os.listdir(config_dir) if f.endswith(".yaml")]

    for filename in config_files:
        config_path = os.path.join(config_dir, filename)
        
        logging.info(f"--- Starting Pipeline for: {filename} ---")
        try:
            config = load_config(config_path)
            orchestrator = Orchestrator(config=config)
            
            # This keeps the same loop alive rather than creating/destroying 
            # the loop 5 times in a row.
            await orchestrator.run() 
            
            logging.info(f"Pipeline execution for {filename} completed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute pipeline for {filename}: {e}")
        
        logging.info("-" * 50)

def main() -> None:
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        # Run the entire batch of pipelines
        asyncio.run(run_pipelines())
    except KeyboardInterrupt:
        logging.info("Pipeline stopped by user.")
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")

if __name__ == "__main__":
    main()